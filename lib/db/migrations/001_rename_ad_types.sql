BEGIN;

ALTER TABLE serp_ads DROP CONSTRAINT IF EXISTS serp_ads_ad_type_check;

UPDATE serp_ads SET ad_type = 'sponsored_product' WHERE ad_type = 'pla';
UPDATE serp_ads SET ad_type = 'sponsored_result'  WHERE ad_type = 'text';

ALTER TABLE serp_ads ADD CONSTRAINT serp_ads_ad_type_check
    CHECK (ad_type IN ('sponsored_result', 'sponsored_product'));

DROP VIEW IF EXISTS v_ad_rates;

ALTER TABLE serp_results RENAME COLUMN text_ad_count TO sponsored_result_count;
ALTER TABLE serp_results RENAME COLUMN pla_count     TO sponsored_product_count;
ALTER TABLE serp_results RENAME COLUMN top_text_ads  TO top_sponsored_results;

CREATE VIEW v_ad_rates AS
SELECT  q.term,
        count(*)                                                AS serps,
        round(100.0 * count(*) FILTER (WHERE v.sponsored_result_count  > 0) / count(*), 1) AS sponsored_result_rate_pct,
        round(100.0 * count(*) FILTER (WHERE v.sponsored_product_count > 0) / count(*), 1) AS sponsored_product_rate_pct,
        round(avg(v.sponsored_result_count), 2)                 AS avg_sponsored_results,
        round(avg(v.sponsored_product_count), 2)                AS avg_sponsored_products,
        round(avg(v.organic_count), 2)                          AS avg_organic
FROM    scrape_requests q
JOIN    serp_results v ON v.request_id = q.request_id
WHERE   q.status = 'ok'
GROUP   BY q.term;

COMMIT;
