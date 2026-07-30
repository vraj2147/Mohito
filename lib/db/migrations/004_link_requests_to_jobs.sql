BEGIN;

ALTER TABLE scrape_requests
    ADD COLUMN IF NOT EXISTS job_id uuid REFERENCES scrape_jobs (job_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_requests_job ON scrape_requests (job_id);

UPDATE scrape_requests q
SET    job_id = j.job_id
FROM   scrape_jobs j
WHERE  j.request_id = q.request_id
  AND  q.job_id IS NULL;

CREATE OR REPLACE VIEW v_job_attempts AS
SELECT  j.job_id,
        j.batch_id,
        j.term,
        j.status          AS job_status,
        j.attempts        AS job_attempts,
        q.request_id,
        q.attempt,
        q.status          AS attempt_status,
        q.worker_id,
        q.duration_ms,
        q.started_at,
        q.html_path
FROM    scrape_jobs j
LEFT JOIN scrape_requests q ON q.job_id = j.job_id
ORDER BY j.batch_id, j.term, q.started_at;

COMMIT;
