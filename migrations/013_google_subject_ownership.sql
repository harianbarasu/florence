CREATE UNIQUE INDEX external_connections_live_google_subject_idx
  ON external_connections (provider, external_account_id)
  WHERE provider = 'google' AND status <> 'revoked' AND external_account_id IS NOT NULL;
