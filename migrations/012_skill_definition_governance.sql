ALTER TABLE skill_versions
  ADD COLUMN definition_digest text;

UPDATE skill_versions version
SET definition_digest = baseline.definition_digest
FROM skills skill,
  (VALUES
    ('coverage.need_interpret', 3, '11672c7a656c3ef49449816c12d54bb78b2c915b86761c71e9a0866fafe7eeb7'),
    ('coverage.commitment_propose', 3, '35a1dc39d5665b172b2629c32942e64977d52740f97a45ce6dbb74cc26b2660e'),
    ('coverage.minimum_disclosure', 1, 'a7b61d8d6c05e88be705fcf18d977b9568347d4725cf978f165e604c986bcef9'),
    ('coverage.outcome_assess', 1, '335a5867f9305c34a803428bcdfccd4d009bd6c063dec42804a4c627fcae65a4'),
    ('coverage.response_interpret', 2, 'debf1ba2eff0cecd9c4aa1c997570036e7e27f080fafb9784ba448c19f406347'),
    ('private_source.reconcile', 1, '998354d86fdc302e53baf7f9f810b381ff73d21d4c1491b30c2fc286c0122c8e'),
    ('general.answer', 1, 'ddb2d444be24d37a239d1c811efce2769100789d9f9edc50b2160dc252570159')
  ) AS baseline(skill_key, version, definition_digest)
WHERE version.skill_id = skill.id
  AND skill.skill_key = baseline.skill_key
  AND version.version = baseline.version;

-- Five superseded versions predate persisted instructions/output-schema names.
-- Preserve their rows and release history, but give them a domain-separated
-- archival identity derived only from the definition fields the database did
-- persist. They can never be executed or promoted as current declarations.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM skill_release_events release
    JOIN skill_versions version ON version.id = release.skill_version_id
    JOIN skills skill ON skill.id = version.skill_id
    WHERE release.active
      AND (skill.skill_key, version.version) IN (
      VALUES
        ('coverage.commitment_propose', 1),
        ('coverage.commitment_propose', 2),
        ('coverage.need_interpret', 1),
        ('coverage.need_interpret', 2),
        ('coverage.response_interpret', 1)
    )
  ) THEN
    RAISE EXCEPTION
      'A legacy skill version is still active; retire its production release explicitly first';
  END IF;
END
$$;

UPDATE skill_versions version
SET status = 'retired',
    definition_digest = encode(
      sha256(
        convert_to(
          jsonb_build_object(
            'archiveFormat', 'florence-legacy-skill-definition-v1',
            'skillKey', skill.skill_key,
            'version', version.version,
            'inputSchema', version.input_schema,
            'outputSchema', version.output_schema,
            'requestedCapabilities', (
              SELECT coalesce(
                jsonb_agg(requested.capability ORDER BY requested.capability),
                '[]'::jsonb
              )
              FROM unnest(version.requested_capabilities) AS requested(capability)
            ),
            'toolCeiling', (
              SELECT coalesce(
                jsonb_agg(tool.capability ORDER BY tool.capability),
                '[]'::jsonb
              )
              FROM unnest(version.tool_ceiling) AS tool(capability)
            ),
            'unavailableFields', jsonb_build_array(
              'instructions',
              'outputSchemaName',
              'purpose',
              'riskClass'
            )
          )::text,
          'UTF8'
        )
      ),
      'hex'
    )
FROM skills skill
WHERE version.skill_id = skill.id
  AND version.definition_digest IS NULL
  AND (skill.skill_key, version.version) IN (
    VALUES
      ('coverage.commitment_propose', 1),
      ('coverage.commitment_propose', 2),
      ('coverage.need_interpret', 1),
      ('coverage.need_interpret', 2),
      ('coverage.response_interpret', 1)
  );

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM skill_versions WHERE definition_digest IS NULL) THEN
    RAISE EXCEPTION
      'Cannot infer an immutable definition digest for a legacy skill version; review it explicitly before migration';
  END IF;
END
$$;

ALTER TABLE skill_versions
  ALTER COLUMN definition_digest SET NOT NULL,
  ADD CONSTRAINT skill_versions_definition_digest_format
    CHECK (definition_digest ~ '^[a-f0-9]{64}$');

CREATE FUNCTION reject_skill_version_definition_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.skill_id IS DISTINCT FROM OLD.skill_id
    OR NEW.version IS DISTINCT FROM OLD.version
    OR NEW.input_schema IS DISTINCT FROM OLD.input_schema
    OR NEW.output_schema IS DISTINCT FROM OLD.output_schema
    OR NEW.requested_capabilities IS DISTINCT FROM OLD.requested_capabilities
    OR NEW.tool_ceiling IS DISTINCT FROM OLD.tool_ceiling
    OR NEW.definition_digest IS DISTINCT FROM OLD.definition_digest
  THEN
    RAISE EXCEPTION 'Governed skill definitions are immutable; create a new version';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER skill_versions_definition_immutable
BEFORE UPDATE ON skill_versions
FOR EACH ROW
EXECUTE FUNCTION reject_skill_version_definition_mutation();

ALTER TABLE worker_attempts
  ADD COLUMN skill_definition_digest text;

UPDATE worker_attempts attempt
SET skill_definition_digest = version.definition_digest
FROM skill_versions version
WHERE version.id = attempt.skill_version_id;

CREATE FUNCTION bind_worker_attempt_skill_definition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  expected_digest text;
BEGIN
  IF TG_OP = 'UPDATE'
    AND (
      NEW.skill_version_id IS DISTINCT FROM OLD.skill_version_id
      OR NEW.skill_definition_digest IS DISTINCT FROM OLD.skill_definition_digest
    )
  THEN
    RAISE EXCEPTION 'A worker attempt skill-definition pin is immutable';
  END IF;

  SELECT definition_digest INTO expected_digest
  FROM skill_versions
  WHERE id = NEW.skill_version_id;

  IF expected_digest IS NULL THEN
    RAISE EXCEPTION 'Worker attempt references an unknown governed skill definition';
  END IF;
  IF NEW.skill_definition_digest IS NULL THEN
    NEW.skill_definition_digest := expected_digest;
  ELSIF NEW.skill_definition_digest IS DISTINCT FROM expected_digest THEN
    RAISE EXCEPTION 'Worker attempt skill-definition digest does not match its version';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER worker_attempts_definition_bound
BEFORE INSERT OR UPDATE ON worker_attempts
FOR EACH ROW
EXECUTE FUNCTION bind_worker_attempt_skill_definition();

ALTER TABLE worker_attempts
  ALTER COLUMN skill_definition_digest SET NOT NULL,
  ADD CONSTRAINT worker_attempts_skill_definition_digest_format
    CHECK (skill_definition_digest ~ '^[a-f0-9]{64}$');
