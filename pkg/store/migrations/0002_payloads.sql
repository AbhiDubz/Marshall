-- 0002_payloads: the command a job executes. Kept out of the jobs
-- table because pkg/types.Job is specified exactly by the brief and
-- carries no payload; the control plane joins it in at dispatch time.

CREATE TABLE IF NOT EXISTS job_payloads (
    job_id TEXT PRIMARY KEY REFERENCES jobs (id),
    cmd    TEXT NOT NULL
);
