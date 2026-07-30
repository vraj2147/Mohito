"""Database-backed batch scraper.

Reads its workload from `search_terms` (via the `v_pending_work` view) and writes
one row per attempt to `scrape_requests`. Raw HTML goes to disk, gzipped, named by
request_id; the row carries the path so any result can be traced back to its page.

    # scrape everything still outstanding in the queue
    python batch_scrape_db.py --headed

    # small trial first
    python batch_scrape_db.py --headed --limit 20

    # recommended for a long run: stop the host idling to sleep mid-job
    caffeinate -i .venv/bin/python batch_scrape_db.py --headed

Resume is implicit. `v_pending_work` diffs each term's target_repeats against its
count of successful attempts, so re-running simply picks up what is missing — there
is no checkpoint file to keep in sync.

Recovery: a crashed or externally-closed browser is detected and the session is
rebuilt, because a dead session otherwise fails every remaining request in the run
(observed: 180 consecutive TargetClosedErrors after one crash).
"""


from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from ad_extractor import extract_ads
from db.store import DEFAULT_DSN, Store, classify_error, write_html
from google_search_html import slugify
from google_session import GoogleSession

_STOP = False


def _handle_sigint(signum, frame):
    global _STOP
    if _STOP:
        raise KeyboardInterrupt
    _STOP = True
    print("\nStopping after this request… (Ctrl-C again to force)", file=sys.stderr)


def looks_empty(html: str) -> bool:
    """A 200 response with no results container — served but useless."""
    return not any(m in html for m in ('id="search"', 'id="rso"', 'id="main"'))


def run(args) -> int:
    store = Store(args.dsn)
    run_id, run_uuid = store.start_run(config=vars(args), notes=args.notes)
    html_root = Path(args.out_dir) / str(run_uuid) / "html"

    jobs = store.build_queue(
        shuffle=not args.no_shuffle, cap=args.limit,
        term=args.term, distinct=args.distinct,
    )
    if not jobs:
        print("Queue is empty — nothing pending in search_terms.", file=sys.stderr)
        store.finish_run(run_id)
        return 1

    distinct = len({j["term"] for j in jobs})
    print(f"Run {run_uuid} (id={run_id})")
    print(f"Workload: {len(jobs)} scrapes over {distinct} distinct terms")
    print(f"HTML dir: {html_root}", flush=True)

    signal.signal(signal.SIGINT, _handle_sigint)

    counts: dict[str, int] = {}
    consecutive_captchas = 0
    consecutive_ok = 0
    escalations = 0
    # Headless is the cheap mode (no window, no display needed). `prefer_headless`
    # means "keep going back to it"; --headed opts out and stays visible throughout.
    prefer_headless = not args.headed
    headless_retry_after = args.deescalate_after
    started = time.time()
    session = GoogleSession(
        headless=not args.headed,
        reject_cookies=not args.accept_cookies,
        profile_dir=Path(args.profile_dir),
        browser_validation=args.browser_validation,
    ).__enter__()

    try:
        for i, job in enumerate(jobs, 1):
            if _STOP:
                break
            term = job["term"]

            for attempt in range(1, args.max_attempts + 1):
                request_id = uuid.uuid4()
                t0 = time.time()
                started_at = datetime.now(timezone.utc)
                status = "unknown"
                extra: dict = {}

                try:
                    html = session.search(term, timeout_ms=args.timeout_ms)
                    status = "empty_serp" if looks_empty(html) else "ok"

                    # HTML is written before the row is inserted: a lost row is
                    # cheap to reconstruct, a lost scrape is not.
                    art = write_html(
                        html, html_root, request_id, slugify(term),
                        compress=not args.no_compress,
                    )
                    extra["html"] = art

                except Exception as exc:
                    status, err_cls, err_msg = classify_error(exc)
                    extra.update(error_class=err_cls, error_message=err_msg)
                    html = None
                    # Persist a block page if one came back, so the row still points
                    # at something inspectable.
                    failed_html = getattr(exc, "html", None)
                    if failed_html:
                        extra["html"] = write_html(
                            failed_html, html_root / status, request_id, slugify(term),
                            compress=not args.no_compress,
                        )

                store.record_attempt(
                    request_id=request_id,
                    run_id=run_id,
                    term_id=job.get("term_id"),
                    term=term,
                    attempt=attempt,
                    status=status,
                    final_url=session.current_url,
                    started_at=started_at,
                    duration_ms=int((time.time() - t0) * 1000),
                    headless=session.headless,
                    **extra,
                )
                counts[status] = counts.get(status, 0) + 1

                # Ad extraction is secondary: the page is already safely on disk, so
                # a parser failure must not fail the scrape or trigger a re-fetch.
                if status == "ok" and html is not None:
                    try:
                        store.record_serp(request_id, extract_ads(html, query=term))
                    except Exception as exc:
                        counts["parse_failed"] = counts.get("parse_failed", 0) + 1
                        print(f"  parse failed for {request_id}: {exc}", file=sys.stderr)

                if status == "ok" or status == "empty_serp":
                    consecutive_captchas = 0
                    consecutive_ok += 1
                    # Drop back to headless once the run looks healthy again, so an
                    # escalation is a temporary rescue rather than a one-way door.
                    # The threshold doubles each time headless fails again, so a
                    # host where headless never works stops paying the retry cost
                    # instead of oscillating every few dozen requests.
                    if (
                        prefer_headless
                        and not session.headless
                        and consecutive_ok >= headless_retry_after
                    ):
                        print(
                            f"  {consecutive_ok} clean requests — trying headless again.",
                            file=sys.stderr,
                        )
                        session.restart(headless=True)
                        consecutive_ok = 0
                    break

                if status == "captcha":
                    consecutive_captchas += 1
                    consecutive_ok = 0
                    if consecutive_captchas >= args.escalate_after and session.headless:
                        headless_retry_after = min(
                            args.max_headless_retry_after, headless_retry_after * 2
                        ) if escalations else args.deescalate_after
                        escalations += 1
                        print(
                            f"  CAPTCHA in headless — relaunching headed "
                            f"(escalation #{escalations}; will retry headless after "
                            f"{headless_retry_after} clean requests).",
                            file=sys.stderr,
                        )
                        session.restart(headless=False)
                        consecutive_captchas = 0
                    else:
                        backoff = min(args.max_backoff,
                                      args.backoff * (2 ** max(0, consecutive_captchas - 1)))
                        print(f"  CAPTCHA — backing off {backoff:.0f}s", file=sys.stderr)
                        time.sleep(backoff)
                    continue

                # A dead browser fails everything that follows unless rebuilt.
                if status == "browser_crash" or not session.is_alive():
                    print("  browser died — rebuilding session", file=sys.stderr)
                    session.restart()
                    continue

                # Transient network/timeout: one short pause, then retry.
                time.sleep(args.retry_delay)

            if i % args.progress_every == 0 or i == len(jobs):
                ok = counts.get("ok", 0)
                elapsed = time.time() - started
                summary = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
                print(
                    f"[{i}/{len(jobs)}] {summary} | ok {ok / max(1, i):.0%} | "
                    f"{elapsed / max(1, i):.1f}s/req",
                    flush=True,
                )

            if not _STOP and i < len(jobs):
                time.sleep(random.uniform(args.min_delay, args.max_delay))

    finally:
        session.close()
        store.finish_run(run_id)

    print("\nRun finished. Metrics:")
    for row in store.query(
        "SELECT status, count(*) n FROM scrape_requests WHERE run_id=%s "
        "GROUP BY status ORDER BY n DESC", (run_id,)
    ):
        print(f"  {row['status']:<15}{row['n']:>6}")
    store.close()
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Scrape the search_terms queue into Postgres.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--dsn", default=DEFAULT_DSN, help="Postgres DSN.")
    ap.add_argument("--out-dir", default="runs/db", help="Root for per-run HTML directories.")
    ap.add_argument("--limit", type=int, default=None, help="Cap the number of scrapes.")
    ap.add_argument("--no-shuffle", action="store_true", help="Run the queue in term order.")
    ap.add_argument("--term", default=None,
                    help="Restrict the queue to this exact search term.")
    ap.add_argument("--distinct", action="store_true",
                    help="At most one scrape per term (breadth instead of repeats).")
    ap.add_argument("--no-compress", action="store_true", help="Store plain .html instead of .html.gz.")
    ap.add_argument("--notes", default=None, help="Free-text note stored on the run row.")

    ap.add_argument("--max-attempts", type=int, default=2, help="Attempts per term (default 2).")
    ap.add_argument("--retry-delay", type=float, default=5.0, help="Pause before a retry.")
    ap.add_argument("--timeout-ms", type=int, default=30000, help="Navigation timeout.")
    ap.add_argument("--min-delay", type=float, default=3.0)
    ap.add_argument("--max-delay", type=float, default=8.0)
    ap.add_argument("--backoff", type=float, default=60.0)
    ap.add_argument("--max-backoff", type=float, default=900.0)
    ap.add_argument("--escalate-after", type=int, default=2,
                    help="Consecutive CAPTCHAs before switching headless->headed.")
    ap.add_argument("--deescalate-after", type=int, default=20,
                    help="Clean requests before dropping headed->headless again (default 20). "
                         "Doubles after each further escalation.")
    ap.add_argument("--max-headless-retry-after", type=int, default=320,
                    help="Ceiling for the headless retry threshold (default 320).")
    ap.add_argument("--progress-every", type=int, default=25)

    ap.add_argument("--headed", action="store_true", help="Visible window (far less likely to be blocked).")
    ap.add_argument("--accept-cookies", action="store_true")
    ap.add_argument("--profile-dir", default=None, help="Chrome profile dir (default: a per-DSN cache dir).")
    ap.add_argument("--browser-validation", default=None)

    args = ap.parse_args()
    if args.profile_dir is None:
        args.profile_dir = str(Path.home() / ".cache" / "google_scraper_db_profile")
    return args


if __name__ == "__main__":
    sys.exit(run(parse_args()))
