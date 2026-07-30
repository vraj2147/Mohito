BEGIN;

ALTER TABLE scrape_requests ALTER COLUMN run_id DROP NOT NULL;

ALTER TABLE scrape_requests
    ADD COLUMN IF NOT EXISTS batch_id bigint REFERENCES scrape_batches (id) ON DELETE SET NULL;

ALTER TABLE scrape_requests
    ADD COLUMN IF NOT EXISTS worker_id text;

CREATE INDEX IF NOT EXISTS idx_requests_batch ON scrape_requests (batch_id);

ALTER TABLE scrape_requests DROP CONSTRAINT IF EXISTS scrape_requests_origin_check;
ALTER TABLE scrape_requests ADD CONSTRAINT scrape_requests_origin_check
    CHECK (run_id IS NOT NULL OR batch_id IS NOT NULL);

CREATE OR REPLACE VIEW v_batch_metrics AS
SELECT  b.id                                                       AS batch_id,
        b.batch_uuid,
        b.notes,
        count(q.request_id)                                        AS attempts,
        count(*) FILTER (WHERE q.status = 'ok')                    AS ok,
        count(*) FILTER (WHERE q.status = 'captcha')               AS captcha,
        count(*) FILTER (WHERE q.status NOT IN ('ok','captcha'))   AS other_errors,
        count(DISTINCT q.term)                                     AS terms,
        count(DISTINCT q.worker_id)                                AS workers,
        round(avg(q.duration_ms) FILTER (WHERE q.status = 'ok'))   AS avg_ok_ms,
        sum(q.html_bytes)                                          AS html_bytes
FROM    scrape_batches b
LEFT JOIN scrape_requests q ON q.batch_id = b.id
GROUP   BY b.id, b.batch_uuid, b.notes;

COMMIT;
