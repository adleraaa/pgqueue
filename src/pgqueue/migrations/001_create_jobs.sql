-- Core jobs table. One row per job for its whole life; state transitions are
-- single-row UPDATEs guarded by a WHERE clause on the expected current state.
CREATE TABLE jobs (
    id               bigserial PRIMARY KEY,
    queue            text        NOT NULL DEFAULT 'default',
    task             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    priority         integer     NOT NULL DEFAULT 0,          -- higher runs first
    state            text        NOT NULL DEFAULT 'queued'
                     CHECK (state IN ('queued', 'running', 'succeeded', 'dead', 'cancelled')),
    idempotency_key  text,
    attempts         integer     NOT NULL DEFAULT 0,          -- incremented on every claim
    max_attempts     integer     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    errors           integer     NOT NULL DEFAULT 0,          -- failed attempts (handler error or lease expiry)
    run_at           timestamptz NOT NULL DEFAULT now(),      -- not claimable before this time
    enqueued_at      timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,                             -- start of the latest attempt
    finished_at      timestamptz,
    -- Seconds between the job becoming runnable and its first claim. Stored once
    -- so the /metrics histogram does not depend on retries rewriting run_at.
    wait_seconds     double precision,
    locked_by        text,
    -- Fencing token: a fresh UUID per claim. ack/fail/heartbeat must present it,
    -- so a worker whose lease already expired cannot overwrite a newer attempt.
    lease_token      uuid,
    lease_expires_at timestamptz,
    last_error       text,
    result           jsonb,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Idempotency keys are unique per queue; NULL keys are unrestricted.
CREATE UNIQUE INDEX jobs_idempotency_key_uq
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Matches the claim query's filter and ORDER BY, so claiming reads the head of
-- a small partial index instead of scanning finished jobs.
CREATE INDEX jobs_claim_idx
    ON jobs (queue, priority DESC, run_at, id)
    WHERE state = 'queued';

-- Used by the reaper to find expired leases.
CREATE INDEX jobs_lease_idx
    ON jobs (lease_expires_at)
    WHERE state = 'running';
