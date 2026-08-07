-- A bounded private-source frontier can be intentionally incomplete (for
-- example, an oversized attachment or an image-only PDF). That is neither a
-- retryable infrastructure failure nor successful interpretation. Keep the
-- honest gap durable and visible without feeding it into automatic redrive.

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs ADD CONSTRAINT jobs_status_check CHECK (
  status IN ('pending', 'leased', 'retry', 'succeeded', 'attention', 'dead', 'cancelled')
);

ALTER TABLE jobs
  ADD COLUMN case_key_digest text
  CHECK (case_key_digest IS NULL OR case_key_digest ~ '^[a-f0-9]{64}$');

CREATE INDEX jobs_private_source_attention_idx
  ON jobs (integration_id, integration_control_epoch, case_key_digest, updated_at DESC)
  WHERE status = 'attention' AND case_key_digest IS NOT NULL;

-- Attachments are independent encrypted source objects, but their authority
-- and lifecycle come from one exact Gmail message. Persist that relationship
-- so message deletion can cryptographically erase every child in one fenced
-- transaction without opening attachment content.
ALTER TABLE source_objects
  ADD COLUMN parent_source_object_id uuid REFERENCES source_objects(id) ON DELETE CASCADE;

CREATE INDEX source_objects_parent_idx
  ON source_objects (parent_source_object_id)
  WHERE parent_source_object_id IS NOT NULL;

-- An approved private-source action is still unsafe to submit after its exact
-- source frontier becomes dirty. Carry the non-content frontier identity onto
-- the effect so claiming, redrive, and the final provider-submit transaction
-- can all fail closed against the same generation.
ALTER TABLE outbox
  -- Deliberately not an FK: frontier/candidate retention erasure must not be
  -- blocked by immutable effect history. A missing row fails every join closed.
  ADD COLUMN private_source_frontier_id uuid,
  ADD COLUMN private_source_frontier_version bigint CHECK (private_source_frontier_version > 0),
  ADD COLUMN private_source_frontier_digest text CHECK (
    private_source_frontier_digest IS NULL OR private_source_frontier_digest ~ '^[a-f0-9]{64}$'
  ),
  ADD COLUMN private_source_generation bigint CHECK (private_source_generation >= 0),
  ADD COLUMN private_source_case_key_digest text CHECK (
    private_source_case_key_digest IS NULL OR private_source_case_key_digest ~ '^[a-f0-9]{64}$'
  ),
  ADD CONSTRAINT outbox_private_source_frontier_fence_complete CHECK (
    (
      private_source_frontier_id IS NULL
      AND private_source_frontier_version IS NULL
      AND private_source_frontier_digest IS NULL
      AND private_source_generation IS NULL
      AND private_source_case_key_digest IS NULL
    )
    OR
    (
      private_source_frontier_id IS NOT NULL
      AND private_source_frontier_version IS NOT NULL
      AND private_source_frontier_digest IS NOT NULL
      AND private_source_generation IS NOT NULL
      AND private_source_case_key_digest IS NOT NULL
      AND integration_id IS NOT NULL
      AND integration_control_epoch IS NOT NULL
      AND person_id IS NOT NULL
      AND person_control_epoch IS NOT NULL
    )
  );

CREATE INDEX outbox_private_source_frontier_fence_idx
  ON outbox (
    private_source_frontier_id,
    private_source_generation,
    status,
    updated_at DESC
  )
  WHERE private_source_frontier_id IS NOT NULL;
