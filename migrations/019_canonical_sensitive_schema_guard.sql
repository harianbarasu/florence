-- The TypeScript rebuild intentionally starts in a fresh PostgreSQL schema. Migrations 001 and
-- 002 are the canonical ciphertext-only baseline, not compatibility migrations for their former
-- plaintext shapes. Fail closed when those filenames were recorded by an obsolete schema instead
-- of allowing the application to start with plaintext columns or incomplete encryption indexes.
DO $$
DECLARE
  legacy_columns text;
  missing_columns text;
  missing_nullable_columns text;
  missing_indexes text;
  missing_constraints text;
BEGIN
  SELECT string_agg(format('%I.%I', expected.table_name, expected.column_name), ', ' ORDER BY 1)
  INTO legacy_columns
  FROM (
    VALUES
      ('household_signals', 'idempotency_key'),
      ('household_signals', 'payload_hash'),
      ('household_signals', 'kind'),
      ('household_signals', 'actor_kind'),
      ('household_signals', 'actor_id'),
      ('household_signals', 'visibility'),
      ('household_signals', 'owner_adult_id'),
      ('household_signals', 'occurred_at'),
      ('household_signals', 'payload'),
      ('provider_inbox', 'idempotency_key'),
      ('provider_inbox', 'payload_hash'),
      ('provider_inbox', 'authentication'),
      ('provider_inbox', 'event_kind'),
      ('provider_inbox', 'occurred_at'),
      ('provider_inbox', 'payload'),
      ('provider_inbox', 'resolution'),
      ('provider_inbox_conflicts', 'payload_hash'),
      ('provider_inbox_conflicts', 'payload'),
      ('household_projections', 'state'),
      ('application_snapshots', 'aggregate'),
      ('application_snapshots', 'projection'),
      ('application_commits', 'idempotency_key'),
      ('application_commits', 'commit_hash'),
      ('application_commits', 'signals'),
      ('application_commits', 'changes'),
      ('application_commits', 'outcome'),
      ('jobs', 'payload'),
      ('scheduled_triggers', 'payload'),
      ('outbox', 'payload'),
      ('outbox', 'payload_hash'),
      ('personal_attention_rule_revisions', 'rule'),
      ('personal_attention_rule_revisions', 'statement'),
      ('calendar_busy_windows', 'starts_at'),
      ('calendar_busy_windows', 'ends_at'),
      ('calendar_busy_windows', 'all_day'),
      ('adults', 'display_name'),
      ('external_connections', 'label'),
      ('external_connections', 'email')
  ) AS expected(table_name, column_name)
  WHERE EXISTS (
    SELECT 1
    FROM information_schema.columns actual
    WHERE actual.table_schema = current_schema()
      AND actual.table_name = expected.table_name
      AND actual.column_name = expected.column_name
  );

  IF legacy_columns IS NOT NULL THEN
    RAISE EXCEPTION USING
      ERRCODE = '55000',
      MESSAGE = 'Florence refused an obsolete plaintext database schema.',
      DETAIL = format('Legacy sensitive columns are still present: %s.', legacy_columns),
      HINT = 'Set FLORENCE_POSTGRES_SCHEMA=florence to a new empty schema, then rerun the migrator. The retired schema is a recovery point, not a compatibility source.';
  END IF;

  SELECT string_agg(format('%I.%I', expected.table_name, expected.column_name), ', ' ORDER BY 1)
  INTO missing_columns
  FROM (
    VALUES
      ('household_signals', 'idempotency_digest', 'text'),
      ('household_signals', 'content_digest', 'text'),
      ('household_signals', 'body_key_id', 'text'),
      ('household_signals', 'body_ciphertext', 'text'),
      ('provider_inbox', 'idempotency_digest', 'text'),
      ('provider_inbox', 'content_digest', 'text'),
      ('provider_inbox', 'routing_digests', 'ARRAY'),
      ('provider_inbox', 'encryption_tenant_kind', 'text'),
      ('provider_inbox', 'encryption_tenant_id', 'text'),
      ('provider_inbox_conflicts', 'content_digest', 'text'),
      ('provider_inbox_conflicts', 'encryption_tenant_kind', 'text'),
      ('provider_inbox_conflicts', 'encryption_tenant_id', 'text'),
      ('household_projections', 'state_key_id', 'text'),
      ('household_projections', 'state_ciphertext', 'text'),
      ('application_snapshots', 'application_phase', 'text'),
      ('application_snapshots', 'snapshot_key_id', 'text'),
      ('application_snapshots', 'snapshot_ciphertext', 'text'),
      ('application_commits', 'idempotency_digest', 'text'),
      ('application_commits', 'content_digest', 'text'),
      ('application_commits', 'body_key_id', 'text'),
      ('application_commits', 'body_ciphertext', 'text'),
      ('jobs', 'payload_digest', 'text'),
      ('scheduled_triggers', 'payload_digest', 'text'),
      ('outbox', 'payload_digest', 'text'),
      ('personal_attention_rule_revisions', 'rule_digest', 'text'),
      ('personal_attention_rule_revisions', 'statement_digest', 'text'),
      ('personal_attention_rule_revisions', 'rule_key_id', 'text'),
      ('personal_attention_rule_revisions', 'rule_ciphertext', 'text'),
      ('personal_attention_rule_revisions', 'statement_key_id', 'text'),
      ('personal_attention_rule_revisions', 'statement_ciphertext', 'text'),
      ('calendar_busy_windows', 'window_key_id', 'text'),
      ('calendar_busy_windows', 'window_ciphertext', 'text'),
      ('calendar_busy_windows', 'candidate_buckets', 'ARRAY'),
      ('adult_identity_details', 'details_key_id', 'text'),
      ('adult_identity_details', 'details_ciphertext', 'text'),
      ('encryption_rotation_runs', 'id', 'uuid'),
      ('encryption_rotation_runs', 'target_key_id', 'text'),
      ('encryption_rotation_runs', 'status', 'text'),
      ('encryption_rotation_runs', 'rows_rewrapped', 'bigint'),
      ('encryption_rotation_runs', 'started_at', 'timestamp with time zone'),
      ('encryption_rotation_runs', 'updated_at', 'timestamp with time zone')
  ) AS expected(table_name, column_name, data_type)
  WHERE NOT EXISTS (
    SELECT 1
    FROM information_schema.columns actual
    WHERE actual.table_schema = current_schema()
      AND actual.table_name = expected.table_name
      AND actual.column_name = expected.column_name
      AND actual.data_type = expected.data_type
      AND actual.is_nullable = 'NO'
  );

  IF missing_columns IS NOT NULL THEN
    RAISE EXCEPTION USING
      ERRCODE = '55000',
      MESSAGE = 'Florence refused a non-canonical sensitive-storage schema.',
      DETAIL = format('Required ciphertext or blind-index columns are missing, nullable, or have the wrong type: %s.', missing_columns),
      HINT = 'Use a new empty FLORENCE_POSTGRES_SCHEMA and rerun every checked-in migration in order.';
  END IF;

  SELECT string_agg(format('%I.%I', expected.table_name, expected.column_name), ', ' ORDER BY 1)
  INTO missing_nullable_columns
  FROM (
    VALUES
      ('jobs', 'connection_id', 'uuid'),
      ('jobs', 'work_kind', 'text'),
      ('jobs', 'payload_key_id', 'text'),
      ('jobs', 'payload_ciphertext', 'text'),
      ('provider_inbox', 'body_key_id', 'text'),
      ('provider_inbox', 'body_ciphertext', 'text'),
      ('provider_inbox_conflicts', 'body_key_id', 'text'),
      ('provider_inbox_conflicts', 'body_ciphertext', 'text'),
      ('scheduled_triggers', 'payload_key_id', 'text'),
      ('scheduled_triggers', 'payload_ciphertext', 'text'),
      ('outbox', 'payload_key_id', 'text'),
      ('outbox', 'payload_ciphertext', 'text'),
      ('external_connections', 'email_digest', 'text'),
      ('external_connections', 'details_key_id', 'text'),
      ('external_connections', 'details_ciphertext', 'text'),
      ('encryption_rotation_runs', 'last_error_code', 'text'),
      ('encryption_rotation_runs', 'completed_at', 'timestamp with time zone')
  ) AS expected(table_name, column_name, data_type)
  WHERE NOT EXISTS (
    SELECT 1
    FROM information_schema.columns actual
    WHERE actual.table_schema = current_schema()
      AND actual.table_name = expected.table_name
      AND actual.column_name = expected.column_name
      AND actual.data_type = expected.data_type
      AND actual.is_nullable = 'YES'
  );

  IF missing_nullable_columns IS NOT NULL THEN
    RAISE EXCEPTION USING
      ERRCODE = '55000',
      MESSAGE = 'Florence refused a non-canonical sensitive-storage schema.',
      DETAIL = format('Required nullable encryption-rotation columns are missing or have the wrong shape: %s.', missing_nullable_columns),
      HINT = 'Use a new empty FLORENCE_POSTGRES_SCHEMA and rerun every checked-in migration in order.';
  END IF;

  SELECT string_agg(expected.index_name, ', ' ORDER BY expected.index_name)
  INTO missing_indexes
  FROM unnest(ARRAY[
    'household_signals_body_key_idx',
    'provider_inbox_routing_idx',
    'provider_inbox_body_key_idx',
    'provider_inbox_conflicts_body_key_idx',
    'household_projections_state_key_idx',
    'application_snapshots_active_idx',
    'application_snapshots_snapshot_key_idx',
    'application_commits_body_key_idx',
    'jobs_connection_idx',
    'jobs_payload_key_idx',
    'scheduled_triggers_payload_key_idx',
    'outbox_payload_key_idx',
    'personal_attention_rule_revisions_rule_key_idx',
    'personal_attention_rule_revisions_statement_key_idx',
    'calendar_busy_windows_candidate_buckets_idx',
    'calendar_busy_windows_window_key_idx',
    'adult_identity_details_key_idx',
    'external_connections_details_key_idx',
    'external_connections_active_email_digest_idx',
    'encryption_rotation_runs_active_idx'
  ]) AS expected(index_name)
  WHERE to_regclass(format('%I.%I', current_schema(), expected.index_name)) IS NULL;

  IF missing_indexes IS NOT NULL THEN
    RAISE EXCEPTION USING
      ERRCODE = '55000',
      MESSAGE = 'Florence refused a non-canonical sensitive-storage schema.',
      DETAIL = format('Required encryption or routing indexes are missing: %s.', missing_indexes),
      HINT = 'Use a new empty FLORENCE_POSTGRES_SCHEMA and rerun every checked-in migration in order.';
  END IF;

  SELECT string_agg(expected.constraint_name, ', ' ORDER BY expected.constraint_name)
  INTO missing_constraints
  FROM unnest(ARRAY[
    'household_signals_idempotency_digest_key',
    'provider_inbox_provider_idempotency_digest_key',
    'provider_inbox_routing_digests_check',
    'provider_inbox_encryption_tenant_kind_check',
    'provider_inbox_body_state_check',
    'provider_inbox_conflicts_inbox_id_content_digest_key',
    'provider_inbox_conflicts_encryption_tenant_kind_check',
    'application_snapshots_application_phase_check',
    'application_commits_household_id_idempotency_digest_key',
    'jobs_payload_state_check',
    'scheduled_triggers_payload_state_check',
    'scheduled_triggers_due_state_check',
    'outbox_payload_state_check',
    'external_connections_details_state_check',
    'external_connections_metadata_account_label_check',
    'personal_attention_rule_revisions_rule_digest_check',
    'personal_attention_rule_revisions_statement_digest_check',
    'personal_attention_rule_revisions_ciphertext_check',
    'encryption_rotation_runs_pkey',
    'encryption_rotation_runs_status_check',
    'encryption_rotation_runs_rows_rewrapped_check',
    'encryption_rotation_runs_check'
  ]) AS expected(constraint_name)
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_constraint actual
    JOIN pg_class relation ON relation.oid = actual.conrelid
    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = current_schema()
      AND actual.conname = expected.constraint_name
  );

  IF missing_constraints IS NOT NULL THEN
    RAISE EXCEPTION USING
      ERRCODE = '55000',
      MESSAGE = 'Florence refused a non-canonical sensitive-storage schema.',
      DETAIL = format('Required encryption, routing, or idempotency constraints are missing: %s.', missing_constraints),
      HINT = 'Use a new empty FLORENCE_POSTGRES_SCHEMA and rerun every checked-in migration in order.';
  END IF;
END
$$;
