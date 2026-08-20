-- 0001_init: jobs, nodes, allocations, append-only event log.

CREATE TABLE IF NOT EXISTS jobs (
    id             TEXT PRIMARY KEY,
    usr            TEXT        NOT NULL,
    priority       INT         NOT NULL,
    cpu_millis     BIGINT      NOT NULL,
    memory_bytes   BIGINT      NOT NULL,
    gres           JSONB       NOT NULL DEFAULT '{}',
    node_count     INT         NOT NULL DEFAULT 1,
    est_runtime_ms BIGINT      NOT NULL,
    submit_at      TIMESTAMPTZ NOT NULL,
    state          TEXT        NOT NULL,
    attempt        INT         NOT NULL DEFAULT 0,
    CONSTRAINT jobs_state_valid CHECK (state IN
        ('PENDING','SCHEDULED','RUNNING','COMPLETED','FAILED','PREEMPTED'))
);

CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs (state, submit_at, id);

CREATE TABLE IF NOT EXISTS nodes (
    id                 TEXT PRIMARY KEY,
    cpu_millis         BIGINT      NOT NULL,
    memory_bytes       BIGINT      NOT NULL,
    gres               JSONB       NOT NULL DEFAULT '{}',
    alloc_cpu_millis   BIGINT      NOT NULL DEFAULT 0,
    alloc_memory_bytes BIGINT      NOT NULL DEFAULT 0,
    alloc_gres         JSONB       NOT NULL DEFAULT '{}',
    last_heartbeat     TIMESTAMPTZ NOT NULL,
    draining           BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS allocations (
    job_id   TEXT        NOT NULL REFERENCES jobs (id),
    attempt  INT         NOT NULL,
    node_ids TEXT[]      NOT NULL,
    start_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (job_id, attempt)
);

CREATE TABLE IF NOT EXISTS events (
    id         BIGSERIAL PRIMARY KEY,
    at         TIMESTAMPTZ NOT NULL,
    job_id     TEXT        NOT NULL DEFAULT '',
    kind       TEXT        NOT NULL,
    from_state TEXT        NOT NULL DEFAULT '',
    to_state   TEXT        NOT NULL DEFAULT '',
    detail     TEXT        NOT NULL DEFAULT ''
);

-- The event log is append-only: reject UPDATE and DELETE at the
-- database layer so no code path can rewrite history.
CREATE OR REPLACE FUNCTION events_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events log is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS events_no_rewrite ON events;
CREATE TRIGGER events_no_rewrite
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_append_only();
