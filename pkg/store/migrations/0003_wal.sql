-- 0003_wal: dispatch write-ahead log. Append-only, like events; the
-- exactly-once protocol replays it after control-plane failover.

CREATE TABLE IF NOT EXISTS dispatch_wal (
    lsn      BIGSERIAL PRIMARY KEY,
    kind     TEXT        NOT NULL, -- DISPATCH_INTENT / DISPATCH_COMMIT / DISPATCH_ABORT
    token    TEXT        NOT NULL,
    job_id   TEXT        NOT NULL,
    attempt  INT         NOT NULL,
    node_ids TEXT[]      NOT NULL DEFAULT '{}',
    at       TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS dispatch_wal_token_idx ON dispatch_wal (token, lsn);

DROP TRIGGER IF EXISTS dispatch_wal_no_rewrite ON dispatch_wal;
CREATE TRIGGER dispatch_wal_no_rewrite
    BEFORE UPDATE OR DELETE ON dispatch_wal
    FOR EACH ROW EXECUTE FUNCTION events_append_only();
