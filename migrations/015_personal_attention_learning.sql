CREATE TABLE personal_learning_releases (
  id text PRIMARY KEY CHECK (id ~ '^sha256:[a-f0-9]{64}$'),
  prompt_digest text NOT NULL CHECK (prompt_digest ~ '^sha256:[a-f0-9]{64}$'),
  schema_version integer NOT NULL CHECK (schema_version > 0),
  corpus_digest text NOT NULL CHECK (corpus_digest ~ '^sha256:[a-f0-9]{64}$'),
  model_route jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('passed', 'failed')),
  case_results jsonb NOT NULL,
  evaluated_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE personal_attention_rule_revisions (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  rule_key text NOT NULL CHECK (rule_key ~ '^sha256:[a-f0-9]{64}$'),
  revision integer NOT NULL CHECK (revision > 0),
  supersedes_revision_id uuid REFERENCES personal_attention_rule_revisions(id),
  status text NOT NULL CHECK (status IN ('active', 'revoked')),
  rule_digest text NOT NULL CONSTRAINT personal_attention_rule_revisions_rule_digest_check
    CHECK (rule_digest ~ '^sha256:[a-f0-9]{64}$'),
  statement_digest text NOT NULL CONSTRAINT personal_attention_rule_revisions_statement_digest_check
    CHECK (statement_digest ~ '^sha256:[a-f0-9]{64}$'),
  rule_key_id text NOT NULL,
  rule_ciphertext text NOT NULL,
  statement_key_id text NOT NULL,
  statement_ciphertext text NOT NULL,
  sensitivity text NOT NULL DEFAULT 'ordinary' CHECK (sensitivity = 'ordinary'),
  source_message_ref text NOT NULL,
  source_event_id text NOT NULL,
  source_content_digest text NOT NULL CHECK (source_content_digest ~ '^sha256:[a-f0-9]{64}$'),
  evaluator_release_id text NOT NULL REFERENCES personal_learning_releases(id),
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (household_id, adult_id, rule_key, revision),
  UNIQUE (household_id, adult_id, source_event_id),
  CHECK ((revision = 1) = (supersedes_revision_id IS NULL)),
  CONSTRAINT personal_attention_rule_revisions_ciphertext_check CHECK (
    rule_key_id <> '' AND rule_ciphertext <> ''
    AND statement_key_id <> '' AND statement_ciphertext <> ''
  )
);

CREATE INDEX personal_attention_rule_revisions_active_idx
  ON personal_attention_rule_revisions (household_id, adult_id, rule_key, revision DESC);

CREATE INDEX personal_attention_rule_revisions_time_idx
  ON personal_attention_rule_revisions (household_id, adult_id, occurred_at, revision);

CREATE INDEX personal_attention_rule_revisions_rule_key_idx
  ON personal_attention_rule_revisions (rule_key_id);

CREATE INDEX personal_attention_rule_revisions_statement_key_idx
  ON personal_attention_rule_revisions (statement_key_id);

CREATE TABLE personal_attention_applications (
  id uuid PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  rule_revision_id uuid NOT NULL REFERENCES personal_attention_rule_revisions(id) ON DELETE CASCADE,
  provider text NOT NULL CHECK (provider IN ('gmail', 'calendar')),
  source_ref text NOT NULL,
  source_digest text NOT NULL CHECK (source_digest ~ '^sha256:[a-f0-9]{64}$'),
  baseline_decision text NOT NULL CHECK (
    baseline_decision IN ('ignore', 'retain_private', 'private_review', 'private_interrupt', 'propose_family_episode')
  ),
  applied_decision text NOT NULL CHECK (
    applied_decision IN ('ignore', 'retain_private', 'private_review', 'private_interrupt', 'propose_family_episode')
  ),
  applied_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (
    rule_revision_id, source_ref, source_digest, baseline_decision, applied_decision
  )
);

CREATE INDEX personal_attention_applications_owner_idx
  ON personal_attention_applications (household_id, adult_id, applied_at DESC);
