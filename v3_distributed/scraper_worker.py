"""Job 2 of 3: consume jobs, scrape, save HTML.

Does no parsing and writes no ad data. Its only database contact is the job
ledger: claim before the request, record the outcome after. Those two statements
cost ~0.3ms against an ~11s scrape, and they are what make a dead worker
recoverable — a job dropped by a killed process would otherwise exist only as an
unacked AMQP message.

    python scraper_worker.py --worker-id w1
    python scraper_worker.py --worker-id w2 --headed      # second worker
    python scraper_worker.py --worker-id w1 --once        # drain then exit

Each worker needs its own Chrome profile directory; the profile lock is exclusive
and two workers sharing one will fail to launch. --worker-id picks the default
path, so distinct ids are all that is required.

Ordering guarantee: the HTML is on disk and the outcome is published to
extract.jobs BEFORE the message is acked. A crash anywhere earlier redelivers the
job; a crash after the ack leaves a `scraped` row the extractor still picks up.
"""


from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import json
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import mq
from db.jobs import JobStore
from db.store import classify_error, write_html
from google_search_html import slugify
from google_session import GoogleSession

_STOP = False
_CONN = None
_CHANNEL = None


def _handle_signal(signum, frame):
    """Break out of start_consuming() promptly.

    pika's blocking consumer sits in a socket read, so setting a flag alone leaves
    an idle worker hanging until the next message. add_callback_threadsafe is the
    supported way to interrupt it from a signal handler.
    """
    global _STOP
    _STOP = True
    if _CONN is not None and _CHANNEL is not None:
        try:
            _CONN.add_callback_threadsafe(_CHANNEL.stop_consuming)
        except Exception:
            pass
    print("\nFinishing current job, then exiting…", file=sys.stderr)


def looks_empty(html: str) -> bool:
    return not any(m in html for m in ('id="search"', 'id="rso"', 'id="main"'))


class Worker:
    def __init__(self, args):
        self.args = args
        self.worker_id = args.worker_id
        self.store = JobStore(args.dsn)
        self.html_root = Path(args.out_dir)
        self.counts: dict[str, int] = {}
        self.consecutive_captchas = 0
        self.consecutive_ok = 0
        self.escalations = 0
        self.prefer_headless = not args.headed
        self.headless_retry_after = args.deescalate_after
        self.processed = 0

        self.session = GoogleSession(
            headless=not args.headed,
            reject_cookies=not args.accept_cookies,
            profile_dir=Path(args.profile_dir),
            browser_validation=args.browser_validation,
        ).__enter__()

    def close(self):
        try:
            self.session.close()
        finally:
            self.store.close()

    def _bump(self, key: str) -> None:
        self.counts[key] = self.counts.get(key, 0) + 1

    def _adjust_mode(self, status: str) -> None:
        """Headless is cheap but more likely to be blocked; escalate to a visible
        window under sustained CAPTCHAs and drop back once healthy."""
        if status in ("ok", "empty_serp"):
            self.consecutive_captchas = 0
            self.consecutive_ok += 1
            if (self.prefer_headless and not self.session.headless
                    and self.consecutive_ok >= self.headless_retry_after):
                print(f"  [{self.worker_id}] {self.consecutive_ok} clean — back to headless",
                      file=sys.stderr)
                self.session.restart(headless=True)
                self.consecutive_ok = 0
            return

        if status == "captcha":
            self.consecutive_captchas += 1
            self.consecutive_ok = 0
            if (self.consecutive_captchas >= self.args.escalate_after
                    and self.session.headless and not self.args.no_escalate):
                self.headless_retry_after = (
                    min(self.args.max_headless_retry_after, self.headless_retry_after * 2)
                    if self.escalations else self.args.deescalate_after
                )
                self.escalations += 1
                print(f"  [{self.worker_id}] CAPTCHA in headless — going headed "
                      f"(retry headless after {self.headless_retry_after} clean)", file=sys.stderr)
                self.session.restart(headless=False)
                self.consecutive_captchas = 0
            else:
                backoff = min(600.0, self.args.captcha_backoff * self.consecutive_captchas)
                print(f"  [{self.worker_id}] CAPTCHA — backing off {backoff:.0f}s", file=sys.stderr)
                time.sleep(backoff)
            return

        if status == "browser_crash" or not self.session.is_alive():
            print(f"  [{self.worker_id}] browser died — rebuilding", file=sys.stderr)
            self.session.restart()

    def handle(self, channel, method, properties, body) -> None:
        global _STOP
        try:
            job = json.loads(body)
            job_id = uuid.UUID(job["job_id"])
        except Exception as exc:
            print(f"  unparseable message, dropping: {exc}", file=sys.stderr)
            channel.basic_ack(method.delivery_tag)
            return

        claimed = self.store.claim(job_id, self.worker_id, lease_seconds=self.args.lease_seconds)
        if claimed is None:
            # Already finished or owned elsewhere — a duplicate delivery.
            self._bump("skipped")
            channel.basic_ack(method.delivery_tag)
            return

        term = claimed["term"]
        request_id = uuid.uuid4()
        started_at = datetime.now(timezone.utc)
        t0 = time.time()
        art = None
        status = "unknown"
        err_cls = err_msg = None

        try:
            html = self.session.search(term, timeout_ms=self.args.timeout_ms)
            status = "empty_serp" if looks_empty(html) else "ok"
            art = write_html(html, self.html_root, request_id, slugify(term),
                             compress=not self.args.no_compress)
        except Exception as exc:
            status, err_cls, err_msg = classify_error(exc)
            failed_html = getattr(exc, "html", None)
            if failed_html:
                art = write_html(failed_html, self.html_root / status, request_id,
                                 slugify(term), compress=not self.args.no_compress)

        duration_ms = int((time.time() - t0) * 1000)
        outcome = {
            "job_id": str(job_id),
            "request_id": str(request_id),
            "batch_id": job.get("batch_id"),
            "term_id": claimed.get("term_id"),
            "term": term,
            "attempt": claimed["attempts"],
            "status": status,
            "error_class": err_cls,
            "error_message": err_msg,
            "final_url": self.session.current_url,
            "started_at": started_at.isoformat(),
            "duration_ms": duration_ms,
            "html_path": str(art.path) if art else None,
            "html_bytes": art.byte_len if art else None,
            "html_sha256": art.sha256 if art else None,
            "headless": self.session.headless,
            "worker_id": self.worker_id,
        }

        succeeded = status in ("ok", "empty_serp")
        if succeeded:
            self.store.mark_scraped(job_id, request_id, outcome["html_path"], status)
        else:
            outcome["job_status"] = self.store.mark_failed(job_id, request_id, status, err_msg)

        # Publish the outcome BEFORE acking. If this raises, the job is redelivered
        # rather than silently lost.
        mq.publish(channel, mq.EXTRACT_QUEUE, outcome, message_id=str(request_id))

        if not succeeded and outcome.get("job_status") == "retry":
            mq.publish(channel, mq.RETRY_QUEUE, job, message_id=str(job_id))

        channel.basic_ack(method.delivery_tag)

        self._bump(status)
        self.processed += 1
        self._adjust_mode(status)

        if self.processed % self.args.progress_every == 0:
            summary = " ".join(f"{k}={v}" for k, v in sorted(self.counts.items()))
            print(f"[{self.worker_id}] {self.processed} done | {summary}", flush=True)

        if _STOP or (self.args.max_jobs and self.processed >= self.args.max_jobs):
            channel.stop_consuming()
            return

        time.sleep(self.args.min_delay + (self.args.max_delay - self.args.min_delay) * 0.5)


def main() -> int:
    ap = argparse.ArgumentParser(description="Consume scrape jobs, save raw HTML.")
    ap.add_argument("--worker-id", default="w1")
    ap.add_argument("--amqp-url", default=mq.DEFAULT_URL)
    ap.add_argument("--dsn", default=None)
    ap.add_argument("--out-dir", default="runs/mq/html")
    ap.add_argument("--profile-dir", default=None)
    ap.add_argument("--lease-seconds", type=int, default=300)
    ap.add_argument("--timeout-ms", type=int, default=30000)
    ap.add_argument("--min-delay", type=float, default=1.0)
    ap.add_argument("--max-delay", type=float, default=3.0)
    ap.add_argument("--max-jobs", type=int, default=None, help="Exit after N jobs.")
    ap.add_argument("--once", action="store_true", help="Exit when the queue is empty.")
    ap.add_argument("--progress-every", type=int, default=10)
    ap.add_argument("--no-compress", action="store_true")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--accept-cookies", action="store_true")
    ap.add_argument("--browser-validation", default=None)
    ap.add_argument("--escalate-after", type=int, default=2)
    ap.add_argument("--no-escalate", action="store_true",
                    help="Never switch to a headed window; back off and stay headless. "
                         "Required when running detached (no window server), where a "
                         "headed launch cannot succeed.")
    ap.add_argument("--captcha-backoff", type=float, default=45.0,
                    help="Seconds to pause after a CAPTCHA when not escalating.")
    ap.add_argument("--deescalate-after", type=int, default=20)
    ap.add_argument("--max-headless-retry-after", type=int, default=320)
    args = ap.parse_args()

    if args.profile_dir is None:
        args.profile_dir = str(Path.home() / ".cache" / f"google_scraper_mq_{args.worker_id}")
    if args.dsn is None:
        from db.store import DEFAULT_DSN
        args.dsn = DEFAULT_DSN

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    global _CONN, _CHANNEL
    conn = mq.connect(args.amqp_url)
    channel = conn.channel()
    _CONN, _CHANNEL = conn, channel
    mq.declare_topology(channel)
    # One unacked message at a time: a worker must not hoard jobs it cannot start,
    # because every held job is invisible to other workers until acked.
    channel.basic_qos(prefetch_count=1)

    worker = Worker(args)
    print(f"[{args.worker_id}] profile={args.profile_dir}")
    print(f"[{args.worker_id}] waiting for jobs on {mq.JOBS_QUEUE}", flush=True)

    try:
        if args.once:
            while not _STOP:
                method, properties, body = channel.basic_get(mq.JOBS_QUEUE, auto_ack=False)
                if method is None:
                    break
                worker.handle(channel, method, properties, body)
                if args.max_jobs and worker.processed >= args.max_jobs:
                    break
        else:
            channel.basic_consume(mq.JOBS_QUEUE, worker.handle, auto_ack=False)
            channel.start_consuming()
    finally:
        worker.close()
        try:
            conn.close()
        except Exception:
            pass

    summary = " ".join(f"{k}={v}" for k, v in sorted(worker.counts.items()))
    print(f"[{args.worker_id}] exited after {worker.processed} job(s) | {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
