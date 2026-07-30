# Google SERP Scraper

Scrapes Google search results at volume, stores the raw HTML, and extracts
sponsored placements (sponsored results and sponsored products) into Postgres.

The repository is organised as three stages, kept side by side deliberately: each
one solves a real problem the previous one hit, and the earlier stages still run.

```
seed_terms.py       shared: fills search_terms — run this first for v2 AND v3
lib/                shared modules used by every stage
v1_sequential/      single process, files on disk
v2_database/        single process, Postgres-backed queue and results
v3_distributed/     RabbitMQ: three independent jobs, horizontally scalable
```

`seed_terms.py` sits at the root because both v2 and v3 depend on it. It populates
`search_terms`; v2 reads that through `v_pending_work`, and v3's loader builds its
RabbitMQ messages from the same view. v1 is the exception — it reads a flat file
(`v1_sequential/queries.txt`) rather than the database.

## Tables

Seven tables, four shared:

| Table | Used by |
|---|---|
| `search_terms` | v2 + v3 — the term catalogue and repeat targets |
| `scrape_requests` | v2 + v3 — one row per attempt (the audit trail) |
| `serp_results` | v2 + v3 — per-SERP ad counts |
| `serp_ads` | v2 + v3 — one row per individual ad |
| `scrape_runs` | v2 only — one row per `batch_scrape_db.py` invocation |
| `scrape_batches` | **v3 only (new)** — one row per loader batch |
| `scrape_jobs` | **v3 only (new)** — the job ledger |

v3 adds two tables and reuses the four shared ones. `scrape_requests.run_id` became
nullable with a `batch_id` alongside it, so an attempt belongs to either a v2 run
or a v3 batch, enforced by a CHECK that one of the two is set.

Eight views: `v_pending_work`, `v_ad_rates`, `v_error_metrics`, `v_run_summary`
(v2), plus `v_job_status`, `v_batch_progress`, `v_stuck_jobs`, `v_batch_metrics`
(v3).

---

## lib/ — shared foundation

| Module | Role |
|---|---|
| `google_search_html.py` | single-query scraper; also the source of the Chrome headers, consent handling and scroll helper |
| `google_session.py` | reusable browser session — one Chrome reused across many searches, with headless/headed switching and crash recovery |
| `ad_extractor.py` | parses a SERP into sponsored results (`[data-text-ad]`) and sponsored products (`.pla-unit`) |
| `db/store.py` | Postgres access: terms, runs, attempts, results |
| `db/jobs.py` | job ledger for the distributed stage |
| `db/schema.sql` + `db/migrations/` | schema and migrations |

### Findings that shaped the design

**Chrome-only headers belong on the navigation request only.** Putting
`x-browser-*` and `Sec-Fetch-*` into `extra_http_headers` stamps them onto every
subresource and XHR, producing fetch requests that claim `Sec-Fetch-Dest:
document` — a contradiction no real browser makes.

**Headless is not the real variable; profile warmth is.** A cold profile was
CAPTCHA'd on request 1. After 74 successful requests warmed the same profile,
headless ran 8/8 clean. Google's block decision leans on cookie history, so a
persistent user-data dir matters more than the rendering mode.

**Ads are JS-hydrated.** `#tads` is empty in the raw HTML, so a plain HTTP fetch
returns a SERP with no ads at all. A browser is mandatory.

**Two link shapes for ad destinations.** Most ads use Google's `/aclk?` click
tracker, but some link straight to the advertiser. Matching only `aclk` silently
lost the destination for 27% of sponsored results.

---

## v1_sequential — the straightforward version

```bash
python v1_sequential/batch_scrape.py \
    --repeat-query "iPhone 16 Pro" --repeat 500 \
    --query-file v1_sequential/queries.txt --total 1000
```

One process: read a query list, scrape, write HTML, append a row to
`results.jsonl`, print a report at the end. Resume replays the JSONL.

**Where it runs out of road:** the checkpoint file is the only record, so "what
is left" means re-reading and diffing a log. Failures are strings rather than
categories. Nothing else can query the results while a run is in progress.

Real numbers from a 1000-request run: 800 ok, 200 errors, 0 CAPTCHAs, 1.9 GB of
HTML. 180 of the 200 errors were consecutive `TargetClosedError` after the browser
died around request 800 — a dead session that was never rebuilt, which is what
motivated `GoogleSession.is_alive()`.

---

## v2_database — Postgres as the source of truth

```bash
python seed_terms.py --distinct 1000 --repeat-term "iPhone 16 Pro" --repeats 500
caffeinate -i python v2_database/batch_scrape_db.py
python v2_database/metrics.py --ads --errors
python v2_database/reextract.py --run 8 9
```

The work queue moves into `search_terms`, and every attempt becomes a row in
`scrape_requests`.

* **One row per attempt, not per success.** A block rate cannot be measured if
  blocked requests leave no trace.
* **`request_id` (uuid4) is generated before the network call** and names the HTML
  file on disk, so a row and its page share one key even if the insert later fails.
* **Resume is a SQL view.** `v_pending_work` diffs `target_repeats` against
  successful attempts — no checkpoint file to keep in sync.
* **Status is a constrained enum** (`ok`, `captcha`, `nav_timeout`, …) so failures
  aggregate; free text is kept only for forensics.
* **HTML is gzipped** — measured 77% saving.

`reextract.py` replays saved HTML into the database, so an extractor fix applies
retroactively without re-scraping. That is how the 27% missing destination URLs
were repaired across already-collected runs.

**Where it runs out of road:** still one process doing everything in sequence. A
parser change means re-running the scraper's own loop, and throughput is capped by
a single browser.

---

## v3_distributed — three jobs over RabbitMQ

```bash
brew services start rabbitmq

python seed_terms.py --distinct 1000                        # 0. fill search_terms
python v3_distributed/warm_profiles.py --workers 2 --headed # 0b. warm each profile
python v3_distributed/pipeline.py --distinct --limit 500 --workers 2   # all three, supervised

# or run the stages by hand
python v3_distributed/loader.py --distinct --limit 500      # 1. fill the queue
python v3_distributed/scraper_worker.py --worker-id w1      # 2. scrape (N workers)
python v3_distributed/extractor_worker.py                   # 3. extract + store

python v3_distributed/loader.py --status                    # ledger + queue depths
python v3_distributed/loader.py --reclaim                   # recover dead workers
python v3_distributed/loader.py --resume                    # republish pending
```

```
loader ──► scrape.jobs ──► scraper_worker ──► extract.jobs ──► extractor_worker
              ▲                  │                                   │
              │                  ▼                                   ▼
        scrape.retry ◄── failure (TTL backoff)              Postgres results
              │
              ▼
        scrape.dead (attempts exhausted)
```

### The failure-visibility problem

A clean split would have the scraper touch no database at all. That breaks the
moment a worker is killed mid-request: the job then exists only as an unacked AMQP
message, and there is no way to ask what is in flight.

The fix is a **job ledger** (`scrape_jobs`). RabbitMQ dispatches; Postgres records.
The scraper writes exactly two rows — claim before, outcome after — costing ~0.3ms
against an ~11s scrape, and three questions always have exact answers:

```sql
SELECT * FROM v_batch_progress;   -- done / waiting / in_flight / dead
SELECT * FROM v_stuck_jobs;       -- workers that died holding a lease
```

### Production details

* **Manual ack, `prefetch=1`.** The message is acked only after the HTML is on
  disk and the outcome is published.
* **Leases.** A claim sets `lease_expires_at`; `--reclaim` returns expired jobs to
  the retry pool. This covers the case AMQP redelivery cannot: a process that
  acked but then died.
* **Delayed retry via TTL queue**, not `nack(requeue=True)` — an immediate requeue
  against a rate-limiting target becomes a hot loop.
* **Durable queues, persistent messages**, so a broker restart loses nothing.
* **Idempotent writes.** `ON CONFLICT DO NOTHING` on the audit row, because AMQP
  is at-least-once and the same outcome can legitimately arrive twice.
* **One Chrome profile per worker** — the profile lock is exclusive, so
  `--worker-id` selects the profile directory.

### Verified crash recovery

Killing a worker with `SIGKILL` mid-scrape:

```
1 in_flight  (stranded, worker_id recorded)   5 queued
→ lease expires, job appears in v_stuck_jobs with overdue_by
→ loader.py --reclaim  →  "Reclaimed 1 expired job(s): 1 requeued, 0 dead"
→ 6 queued
```

Nothing lost, and the stranded job was visible and attributable the whole time.

### Cold profiles are the real cost of a new worker

Chrome's profile lock is exclusive, so every worker needs its own user-data dir
(`--worker-id` selects it). A brand-new profile is CAPTCHA'd almost immediately: in
a two-worker run, w1 (warm) scraped 13 pages while w2 (fresh) failed on its first
request and never recovered. `warm_profiles.py` walks each profile through ordinary
searches until it strings together clean requests. After warming, the same two
workers completed a 40-job batch with 3 CAPTCHAs between them.

### Supervision has to detect hung workers, not just dead ones

The first full run stalled at 13/40 with both workers alive and idle. They had
escalated to headed, and a headed launch needs a window-server connection that a
subprocess-spawned worker does not have — Playwright blocked instead of erroring.
Three changes came out of that:

* `GoogleSession` launches with an explicit timeout and falls back to headless
  when a headed launch fails.
* Pipeline-spawned workers get `--no-escalate` and back off instead, since headed
  cannot work detached.
* The supervisor judges liveness from `scrape_requests.started_at` per worker and
  restarts anything silent past `--stall-seconds`. Process liveness alone misses a
  frozen worker entirely.

### Verified full run

40 jobs, 2 workers, resumed from a partially complete batch:

```
 batch  loaded  extracted   left  in_flight  to_extract   dead    pct
     3      40         40      0          0           0      0 100.0%

  ok       41   avg  3711ms   2 workers
  captcha   3   avg 11541ms   2 workers
  unknown   1
  total    45   40 distinct terms   101 MB html
```

45 attempts for 40 jobs: the failures were retried through the TTL queue and all
recovered, none dead.

### Honest limits

Horizontal scaling is real but **blocking is the binding constraint, not
throughput**. Four workers from one IP get blocked roughly four times faster;
this architecture pays off with residential proxies, one per worker. Extraction is
~0.2s of an ~11s request, so moving it to a separate process buys decoupling and
independent redeploy, not speed.

---

## Setup

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/playwright install chromium

brew services start postgresql@14
createdb google_scraper
psql -d google_scraper -f lib/db/schema.sql
for f in lib/db/migrations/*.sql; do psql -d google_scraper -f "$f"; done

brew services start rabbitmq        # v3 only
```

Environment: `SCRAPER_DSN` (default `postgresql:///google_scraper`),
`SCRAPER_AMQP_URL` (default `amqp://guest:guest@localhost:5672/%2F`).

### RabbitMQ access

Homebrew's RabbitMQ ships with the stock local account and no password set by you:

| | |
|---|---|
| username | `guest` |
| password | `guest` |
| AMQP port | `5672` |
| management UI | <http://localhost:15672> |

`guest` is restricted to localhost connections by RabbitMQ's own default, which is
why it is safe here and would not be on a shared host. For anything non-local,
create a real user and drop guest:

```bash
rabbitmqctl add_user scraper '<password>'
rabbitmqctl set_permissions -p / scraper '.*' '.*' '.*'
rabbitmqctl set_user_tags scraper administrator
rabbitmqctl delete_user guest
export SCRAPER_AMQP_URL='amqp://scraper:<password>@localhost:5672/%2F'
```

## Measured performance

| | |
|---|---|
| Per successful scrape | ~11s wall |
| — artificial delay | ~5.5s (62% of wall time) |
| — `scroll_to_bottom` | ~2.2s (58% of browser time) |
| — navigation | ~0.7s |
| — parse + gzip + DB | ~0.3s |
| Database round-trip | 0.13ms (≈0.005% of a request) |
| gzip saving on HTML | 77% |

The two levers that matter are the inter-request delay and the scroll; the
database and parsing are noise.
