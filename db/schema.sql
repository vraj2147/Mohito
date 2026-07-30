-- Google scraper schema (PostgreSQL 14+).
--
-- Design notes:
--
-- * scrape_requests holds ONE ROW PER ATTEMPT, not per success. Failures are the
--   whole point of the table: you cannot measure a block rate if blocked requests
--   leave no trace. Every attempt gets its request_id before the network call.
--
-- * request_id is generated client-side (uuid4) so the HTML file on disk can be
--   named after it. That makes the disk artefact and the DB row joinable by one
--   key even if the INSERT later fails.
--
-- * Raw HTML lives on disk (gzipped), not in the database. The row carries
--   html_path + html_sha256 + html_bytes, which keeps the DB small while still
--   letting you go from a result row straight to the raw page.
--
-- * status is constrained to a small vocabulary. Free-text errors do not
--   aggregate, so the classified status is what metrics are built on and
--   error_class/error_message are kept only for forensics.

BEGIN;

-- ---------------------------------------------------------------- work queue
CREATE TABLE IF NOT EXISTS search_terms (
    id              bigserial PRIMARY KEY,
    term            text        NOT NULL,
    locale          text        NOT NULL DEFAULT 'en-GB',
    -- How many successful scrapes we want of this term. The "same term 500x"
    -- workload is a single row with target_repeats = 500.
    target_repeats  integer     NOT NULL DEFAULT 1 CHECK (target_repeats > 0),
    priority        integer     NOT NULL DEFAULT 0,
    category        text,
    intent          text,       -- 'transactional' | 'informational' | NULL
    active          boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (term, locale)
);

CREATE INDEX IF NOT EXISTS idx_search_terms_active
    ON search_terms (active, priority DESC, id);

-- ------------------------------------------------------------------- runs
CREATE TABLE IF NOT EXISTS scrape_runs (
    id           bigserial   PRIMARY KEY,
    run_uuid     uuid        NOT NULL UNIQUE,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    config       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    git_sha      text,
    notes        text
);

-- --------------------------------------------------------------- attempts
CREATE TABLE IF NOT EXISTS scrape_requests (
    request_id    uuid        PRIMARY KEY,
    run_id        bigint      NOT NULL REFERENCES scrape_runs (id) ON DELETE CASCADE,
    term_id       bigint      REFERENCES search_terms (id) ON DELETE SET NULL,
    -- Denormalised so the audit log stays readable even if a term is deleted.
    term          text        NOT NULL,
    attempt       integer     NOT NULL DEFAULT 1,

    status        text        NOT NULL CHECK (status IN (
                      'ok', 'captcha', 'nav_timeout', 'network_error',
                      'dns_error', 'consent_failed', 'empty_serp',
                      'parse_error', 'browser_crash', 'unknown'
                  )),
    error_class   text,
    error_message text,
    final_url     text,

    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    duration_ms   integer,

    html_path     text,
    html_bytes    integer,
    html_sha256   text,

    headless      boolean,
    proxy         text
);

CREATE INDEX IF NOT EXISTS idx_requests_run     ON scrape_requests (run_id);
CREATE INDEX IF NOT EXISTS idx_requests_status  ON scrape_requests (status);
CREATE INDEX IF NOT EXISTS idx_requests_term    ON scrape_requests (term_id, status);
CREATE INDEX IF NOT EXISTS idx_requests_sha     ON scrape_requests (html_sha256);

-- ------------------------------------------------------- parsed per-SERP
CREATE TABLE IF NOT EXISTS serp_results (
    request_id     uuid    PRIMARY KEY
                           REFERENCES scrape_requests (request_id) ON DELETE CASCADE,
    sponsored_result_count  integer NOT NULL DEFAULT 0,
    sponsored_product_count integer NOT NULL DEFAULT 0,
    total_ads               integer NOT NULL DEFAULT 0,
    top_sponsored_results   integer NOT NULL DEFAULT 0,
    organic_count           integer NOT NULL DEFAULT 0
);

-- ------------------------------------------------------------ individual ads
CREATE TABLE IF NOT EXISTS serp_ads (
    id              bigserial PRIMARY KEY,
    request_id      uuid    NOT NULL
                            REFERENCES scrape_requests (request_id) ON DELETE CASCADE,
    ad_type         text    NOT NULL CHECK (ad_type IN ('sponsored_result', 'sponsored_product')),
    placement       text,
    slot            integer,
    title           text,
    advertiser      text,
    price           text,
    destination_url text
);

CREATE INDEX IF NOT EXISTS idx_ads_request    ON serp_ads (request_id);
CREATE INDEX IF NOT EXISTS idx_ads_advertiser ON serp_ads (ad_type, advertiser);

-- ------------------------------------------------------------------- views

-- What still needs scraping: terms whose successful count is short of target.
CREATE OR REPLACE VIEW v_pending_work AS
SELECT  t.id            AS term_id,
        t.term,
        t.locale,
        t.target_repeats,
        COALESCE(d.ok_count, 0)                      AS ok_count,
        t.target_repeats - COALESCE(d.ok_count, 0)   AS remaining,
        t.priority
FROM    search_terms t
LEFT JOIN (
        SELECT term_id, count(*) AS ok_count
        FROM   scrape_requests
        WHERE  status = 'ok'
        GROUP  BY term_id
) d ON d.term_id = t.id
WHERE   t.active
  AND   t.target_repeats > COALESCE(d.ok_count, 0);

-- Error metrics: the answer to "which searches failed, and how".
CREATE OR REPLACE VIEW v_error_metrics AS
SELECT  q.run_id,
        q.status,
        count(*)                                       AS n,
        round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY q.run_id), 2) AS pct
FROM    scrape_requests q
GROUP   BY q.run_id, q.status;

-- One row per run with the headline numbers.
CREATE OR REPLACE VIEW v_run_summary AS
SELECT  s.id                AS run_id,
        s.run_uuid,
        s.started_at,
        s.finished_at,
        count(q.request_id)                                        AS attempts,
        count(*) FILTER (WHERE q.status = 'ok')                    AS ok,
        count(*) FILTER (WHERE q.status = 'captcha')               AS captcha,
        count(*) FILTER (WHERE q.status NOT IN ('ok', 'captcha'))  AS other_errors,
        round(avg(q.duration_ms) FILTER (WHERE q.status = 'ok'))   AS avg_ok_ms,
        sum(q.html_bytes)                                          AS html_bytes
FROM    scrape_runs s
LEFT JOIN scrape_requests q ON q.run_id = s.id
GROUP   BY s.id, s.run_uuid, s.started_at, s.finished_at;

-- Ad rates per term, computed over successful scrapes only.
CREATE OR REPLACE VIEW v_ad_rates AS
SELECT  q.term,
        count(*)                                                AS serps,
        round(100.0 * count(*) FILTER (WHERE v.sponsored_result_count > 0) / count(*), 1) AS sponsored_result_rate_pct,
        round(100.0 * count(*) FILTER (WHERE v.sponsored_product_count      > 0) / count(*), 1) AS sponsored_product_rate_pct,
        round(avg(v.sponsored_result_count), 2)                          AS avg_sponsored_results,
        round(avg(v.sponsored_product_count), 2)                              AS avg_sponsored_products,
        round(avg(v.organic_count), 2)                          AS avg_organic
FROM    scrape_requests q
JOIN    serp_results v ON v.request_id = q.request_id
WHERE   q.status = 'ok'
GROUP   BY q.term;

COMMIT;
