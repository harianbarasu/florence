CREATE TABLE conversation_messages (
  message_ref text PRIMARY KEY,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  target_scope text NOT NULL CHECK (target_scope IN ('personal', 'household')),
  target_adult_id uuid REFERENCES adults(id) ON DELETE CASCADE,
  provider text NOT NULL CHECK (provider IN ('linq')),
  external_chat_id text NOT NULL,
  provider_message_id text NOT NULL,
  message_class text NOT NULL CHECK (
    message_class IN (
      'onboarding', 'private_review', 'private_interrupt', 'promotion_request',
      'clarifying_question', 'status', 'daily_brief', 'reminder',
      'missed_window', 'approval_request'
    )
  ),
  response_context jsonb,
  app_idempotency_key text NOT NULL,
  sent_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, external_chat_id, provider_message_id),
  UNIQUE (household_id, app_idempotency_key),
  CHECK (response_context IS NULL OR jsonb_typeof(response_context) = 'object'),
  CHECK (
    (target_scope = 'personal' AND target_adult_id IS NOT NULL)
    OR (target_scope = 'household' AND target_adult_id IS NULL)
  )
);

CREATE INDEX conversation_messages_household_sent_idx
  ON conversation_messages (household_id, sent_at DESC);

CREATE TABLE conversation_feedback (
  id uuid PRIMARY KEY,
  message_ref text NOT NULL REFERENCES conversation_messages(message_ref) ON DELETE CASCADE,
  household_id uuid NOT NULL REFERENCES households(id) ON DELETE CASCADE,
  actor_adult_id uuid NOT NULL REFERENCES adults(id) ON DELETE CASCADE,
  feedback_ref text NOT NULL,
  feedback_kind text NOT NULL CHECK (feedback_kind IN ('acknowledgement', 'other')),
  active boolean NOT NULL,
  occurred_at timestamptz NOT NULL,
  source_event_id text NOT NULL CHECK (source_event_id <> ''),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (message_ref, actor_adult_id, feedback_ref)
);

CREATE INDEX conversation_feedback_household_active_idx
  ON conversation_feedback (household_id, occurred_at DESC)
  WHERE active;
