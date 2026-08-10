-- A disconnect fences local source authority immediately, but the encrypted
-- credential remains available only until Google's grant-revocation receipt is
-- reconciled. The pending state prevents reconnect from replacing that token
-- before the durable external effect finishes.

ALTER TABLE integrations
  DROP CONSTRAINT integrations_status_check,
  ADD CONSTRAINT integrations_status_check CHECK (
    status IN ('active', 'paused', 'reauth_required', 'revocation_pending', 'revoked', 'error')
  ),
  ADD COLUMN account_label_ciphertext bytea,
  ADD COLUMN account_label_key_version text,
  ADD COLUMN last_authorized_capabilities text[] NOT NULL,
  ADD CONSTRAINT integrations_account_label_key_pair CHECK (
    (account_label_ciphertext IS NULL) = (account_label_key_version IS NULL)
  ),
  ADD CONSTRAINT integrations_last_authorized_capabilities_check CHECK (
    last_authorized_capabilities <@ ARRAY['mail', 'calendar']::text[]
      AND cardinality(last_authorized_capabilities) BETWEEN 1 AND 2
  );

-- 022's global provider-subject ownership index covers every status except
-- `revoked`, so `revocation_pending` remains globally unique until settlement.

-- The human-facing account label is not a provider credential. Keeping it in
-- a separately encrypted column lets an owner distinguish disconnected source
-- accounts after the provider token has been retired. Person deletion erases it.
-- The exact last successful capability set is preserved for an explicit
-- reconnect; historical grant rows cannot distinguish removal from revocation.
