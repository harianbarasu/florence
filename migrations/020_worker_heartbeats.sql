CREATE TABLE worker_heartbeats (
  name text PRIMARY KEY,
  renewed_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
