BEGIN;

CREATE TABLE IF NOT EXISTS scrape_batches (
    id          bigserial   PRIMARY KEY,
    batch_uuid  uuid        NOT NULL UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now(),
    closed_at   timestamptz,
    config      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    notes       text
);

CREATE TABLE IF NOT EXISTS scrape_jobs (
    job_id           uuid        PRIMARY KEY,
    batch_id         bigint      NOT NULL REFERENCES scrape_batches (id) ON DELETE CASCADE,
    term_id          bigint      REFERENCES search_terms (id) ON DELETE SET NULL,
    term             text        NOT NULL,
    locale           text        NOT NULL DEFAULT 'en-GB',

    status           text        NOT NULL DEFAULT 'pending' CHECK (status IN (
                         'pending', 'queued', 'in_flight', 'scraped',
                         'extracted', 'retry', 'dead'
                     )),
    attempts         integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL DEFAULT 3,

    worker_id        text,
    lease_expires_at timestamptz,

    request_id       uuid,
    last_status      text,
    last_error       text,
    html_path        text,

    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    published_at     timestamptz,
    scraped_at       timestamptz,
    extracted_at     timestamptz
);

CREATE INDEX IF NOT EXISTS idx_jobs_status       ON scrape_jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_batch        ON scrape_jobs (batch_id, status);
CREATE INDEX IF NOT EXISTS idx_jobs_lease        ON scrape_jobs (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_jobs_term         ON scrape_jobs (term_id);

CREATE OR REPLACE VIEW v_job_status AS
SELECT  b.id                                   AS batch_id,
        b.batch_uuid,
        b.notes,
        j.status,
        count(*)                               AS n,
        round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY b.id), 1) AS pct
FROM    scrape_batches b
JOIN    scrape_jobs j ON j.batch_id = b.id
GROUP   BY b.id, b.batch_uuid, b.notes, j.status;

CREATE OR REPLACE VIEW v_batch_progress AS
SELECT  b.id                                                        AS batch_id,
        b.batch_uuid,
        b.notes,
        b.created_at,
        b.closed_at,
        count(*)                                                    AS total,
        count(*) FILTER (WHERE j.status = 'extracted')              AS done,
        count(*) FILTER (WHERE j.status IN ('pending','queued','retry')) AS waiting,
        count(*) FILTER (WHERE j.status = 'in_flight')              AS in_flight,
        count(*) FILTER (WHERE j.status = 'scraped')                AS awaiting_extract,
        count(*) FILTER (WHERE j.status = 'dead')                   AS dead,
        round(100.0 * count(*) FILTER (WHERE j.status = 'extracted') / count(*), 1) AS pct_done
FROM    scrape_batches b
JOIN    scrape_jobs j ON j.batch_id = b.id
GROUP   BY b.id, b.batch_uuid, b.notes, b.created_at, b.closed_at;

CREATE OR REPLACE VIEW v_stuck_jobs AS
SELECT  job_id, batch_id, term, worker_id, attempts, max_attempts,
        lease_expires_at,
        now() - lease_expires_at AS overdue_by
FROM    scrape_jobs
WHERE   status = 'in_flight'
  AND   lease_expires_at < now();

COMMIT;
