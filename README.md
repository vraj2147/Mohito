# Google SERP Scraper

Scrapes Google results at volume, stores the raw HTML, and extracts sponsored
placements into Postgres.

Three stages, kept side by side — each solves a problem the previous one hit, and
all three still run.

```
seed_terms.py       fills search_terms (needed by v2 and v3)
doctor.py           diagnose CAPTCHAs and environment problems
lib/                browser session, ad extractor, Postgres access
v1_sequential/      one process, JSONL checkpoint
v2_database/        one process, Postgres queue + per-attempt audit
v3_distributed/     RabbitMQ: loader -> scraper workers -> extractor
```

## Setup

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/playwright install chromium

brew services start postgresql@14 && createdb google_scraper
psql -d google_scraper -f lib/db/schema.sql
for f in lib/db/migrations/*.sql; do psql -d google_scraper -f "$f"; done
brew services start rabbitmq          # v3 only
```

RabbitMQ: `guest` / `guest`, AMQP `5672`, UI <http://localhost:15672>.
Env: `SCRAPER_DSN`, `SCRAPER_AMQP_URL`.

## Running

```bash
python seed_terms.py --distinct 1000 --repeat-term "iPhone 16 Pro" --repeats 500
```

**v1** — flat file in, files out:
```bash
python v1_sequential/batch_scrape.py --query-file v1_sequential/queries.txt --total 100
```

**v2** — one process, resumable from the database:
```bash
python v2_database/batch_scrape_db.py --limit 100
python v2_database/metrics.py --ads --errors
python v2_database/reextract.py --all          # re-parse saved HTML, no scraping
```

**v3** — all three jobs, supervised:
```bash
python v3_distributed/warm_profiles.py --workers 2 --headed   # once per new worker
python v3_distributed/pipeline.py --distinct --limit 50 --workers 2
```

**v3 — stages separately:**
```bash
python v3_distributed/loader.py --status              # queue depths + ledger (start here)
python v3_distributed/loader.py --distinct --limit 50
python v3_distributed/scraper_worker.py --worker-id w1
python v3_distributed/extractor_worker.py

python v3_distributed/loader.py --resume              # republish pending
python v3_distributed/loader.py --reclaim             # recover dead workers
```

Each worker needs a distinct `--worker-id`: it selects the Chrome profile dir, and
the profile lock is exclusive.

## Testing a change

| Changed | Test | Cost |
|---|---|---|
| extractor | `python lib/ad_extractor.py <saved.html.gz>` | none |
| extractor, in bulk | `python v2_database/reextract.py --all --dry-run` | none |
| DB layer | direct `JobStore` calls in a REPL | none |
| browser/session | `python lib/google_search_html.py "air fryer" -o /tmp/t.html` | 1 request |
| worker | `loader.py --limit 3` then `scraper_worker.py --once --max-jobs 3` | 3 requests |

`reextract.py --all --dry-run` is the regression check: it reports how many counts
*would* change without writing. Hundreds of saved pages mean most work needs no
scraping at all.

`--max-jobs` bounds a worker so a test cannot turn into a thousand requests.

## When you get CAPTCHAs

```bash
python doctor.py                 # services, profile warmth, blocking rates
python doctor.py --open-captcha  # dump the actual block page
```

The usual cause is a **cold profile**, not volume. Google's decision leans on cookie
history, and a profile without an `NID` cookie looks brand new. Measured here:

| | attempts | CAPTCHA rate |
|---|---|---|
| headed | 52 | **0%** |
| headless, warm-ish | 48 | 25% |
| headless, fresh profile | 33 | 33% |

A CAPTCHA on attempt 1 is the cold-profile signature, not rate limiting. Fixes, in
order: `warm_profiles.py --headed`, then slower delays, then wait — blocking is
per-IP and decays over 15–30 minutes.

## Design notes

* **One row per attempt**, not per success — a block rate is unmeasurable if blocked
  requests leave no trace.
* **`request_id` is generated before the network call** and names the HTML file, so a
  row and its page share one key. `job_id` groups the attempts of one unit of work.
* **Raw HTML is written before anything parses it**, gzipped (77% saving). A parser
  bug can never cost a scrape; `reextract.py` replays.
* **Resume is a SQL view** (`v_pending_work`), not a checkpoint file.
* **v3 acks last**: claim → scrape → save → publish → ack. Crash before the ack and
  RabbitMQ redelivers; crash after and the ledger already says `scraped`.
* **Leases**, not just acks. `--reclaim` recovers jobs whose worker died, and a claim
  is refused while another worker's lease is live.
* **Retry via a TTL queue**, not `nack(requeue=True)` — an immediate requeue against
  a rate-limiting target is a hot loop.
* **Ads are JS-hydrated** — `#tads` is empty in the raw HTML, so a plain HTTP fetch
  returns no ads. A browser is mandatory.

## Tables

`search_terms`, `scrape_requests`, `serp_results`, `serp_ads` are shared.
`scrape_runs` is v2 only; `scrape_batches` and `scrape_jobs` are v3 only.
`scrape_requests.run_id` is nullable with `batch_id` alongside — an attempt belongs
to a v2 run or a v3 batch.

Views: `v_pending_work`, `v_batch_progress`, `v_stuck_jobs`, `v_job_attempts`,
`v_ad_rates`, `v_error_metrics`.

## Limits

Blocking is the binding constraint, not throughput. Four workers from one IP get
blocked roughly four times faster; the distributed stage pays off with one
residential proxy per worker. Extraction is ~0.2s of an ~11s request, so splitting it
out buys decoupling and independent redeploys, not speed.

Per successful scrape: ~11s wall — ~5.5s artificial delay, ~2.2s scrolling, ~0.7s
navigation, ~0.3s parse + gzip + DB. The delay and the scroll are the only levers
that matter.
