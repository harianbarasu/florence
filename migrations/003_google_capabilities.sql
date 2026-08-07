-- Google accounts are durable connections with independently granted mail and
-- calendar capabilities. OAuth attempts retain the exact account intent so a
-- callback cannot widen what the person authorized before leaving Florence.

ALTER TABLE integrations
  ADD COLUMN account_kind text;

UPDATE integrations
SET account_kind = 'personal_family'
WHERE account_kind IS NULL;

ALTER TABLE integrations
  ALTER COLUMN account_kind SET NOT NULL,
  ADD CONSTRAINT integrations_account_kind_check CHECK (
    account_kind IN ('personal_family', 'work')
  );

CREATE TABLE integration_capabilities (
  integration_id uuid NOT NULL REFERENCES integrations(id) ON DELETE CASCADE,
  capability text NOT NULL CHECK (capability IN ('mail', 'calendar')),
  status text NOT NULL CHECK (status IN ('active', 'revoked')),
  granted_at timestamptz NOT NULL,
  revoked_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (integration_id, capability),
  CHECK (
    (status = 'active' AND revoked_at IS NULL)
    OR (status = 'revoked' AND revoked_at IS NOT NULL)
  )
);

INSERT INTO integration_capabilities (
  integration_id, capability, status, granted_at, revoked_at, updated_at
)
SELECT integration.id, capability.name, 'active', integration.connected_at, null, integration.updated_at
FROM integrations integration
CROSS JOIN (VALUES ('mail'), ('calendar')) AS capability(name)
WHERE integration.provider = 'google' AND integration.status <> 'revoked';

ALTER TABLE oauth_attempts
  ADD COLUMN requested_capabilities text[],
  ADD COLUMN account_kind text,
  ADD COLUMN initiating_session_id uuid REFERENCES person_sessions(id) ON DELETE CASCADE;

UPDATE oauth_attempts
SET requested_capabilities = ARRAY['mail', 'calendar']::text[],
    account_kind = 'personal_family'
WHERE requested_capabilities IS NULL OR account_kind IS NULL;

-- An authorization request must return to the same Florence browser session.
-- Pre-migration attempts are short-lived setup state and cannot be bound safely.
DELETE FROM oauth_attempts WHERE consumed_at IS NULL;

ALTER TABLE oauth_attempts
  ALTER COLUMN requested_capabilities SET NOT NULL,
  ALTER COLUMN account_kind SET NOT NULL,
  ADD CONSTRAINT oauth_attempts_requested_capabilities_check CHECK (
    cardinality(requested_capabilities) BETWEEN 1 AND 2
    AND requested_capabilities <@ ARRAY['mail', 'calendar']::text[]
    AND (
      cardinality(requested_capabilities) = 1
      OR requested_capabilities[1] <> requested_capabilities[2]
    )
  ),
  ADD CONSTRAINT oauth_attempts_account_kind_check CHECK (
    account_kind IN ('personal_family', 'work')
  ),
  ADD CONSTRAINT oauth_attempts_initiating_session_check CHECK (
    consumed_at IS NOT NULL OR initiating_session_id IS NOT NULL
  );

-- Jobs carry an integration fence as well as the existing person fence. Lower
-- numeric priority claims first; 100 preserves the historical queue ordering.
ALTER TABLE jobs
  ADD COLUMN integration_id uuid REFERENCES integrations(id) ON DELETE CASCADE,
  ADD COLUMN integration_control_epoch bigint CHECK (integration_control_epoch > 0),
  ADD COLUMN priority integer;

UPDATE jobs
SET priority = 100
WHERE priority IS NULL;

ALTER TABLE jobs
  ALTER COLUMN priority SET DEFAULT 100,
  ALTER COLUMN priority SET NOT NULL,
  ADD CONSTRAINT jobs_integration_fence_complete CHECK (
    (integration_id IS NULL) = (integration_control_epoch IS NULL)
  ),
  ADD CONSTRAINT jobs_priority_nonnegative CHECK (priority >= 0);

-- Pre-capability Google work cannot be given a trustworthy integration fence
-- from its encrypted payload. Retire only the still-runnable legacy chain; the
-- watchdog reseeds every active capability with the current integration epoch.
UPDATE jobs
SET status = 'cancelled',
    lease_owner = null,
    lease_token = null,
    lease_expires_at = null,
    last_error_code = 'superseded_by_integration_fence',
    updated_at = now()
WHERE job_kind LIKE 'google.%'
  AND integration_id IS NULL
  AND status IN ('pending', 'retry', 'leased');

DROP INDEX jobs_claim_idx;

CREATE INDEX jobs_claim_idx
  ON jobs (priority, available_at, created_at)
  WHERE status IN ('pending', 'retry', 'leased');

CREATE INDEX jobs_integration_chain_idx
  ON jobs (integration_id, integration_control_epoch, job_kind, updated_at DESC)
  WHERE integration_id IS NOT NULL;
