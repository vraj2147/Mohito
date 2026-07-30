"""Job 3 of 3: consume scrape outcomes, extract ads, write results.

Writes the `scrape_requests` audit row for every attempt — successes and failures
alike — then parses the saved HTML into `serp_results` and `serp_ads` and closes
the job in the ledger.

    python extractor_worker.py
    python extractor_worker.py --once          # drain then exit
    python extractor_worker.py --workers 2     # (run two processes)

Runs entirely offline against saved HTML, so it can be stopped, fixed and
restarted without touching Google. Outcomes wait durably in extract.jobs while it
is down.

A parse failure is not a scrape failure: the audit row is still written and the
HTML is still on disk, so the job is marked extracted and the error recorded. Work
is never re-fetched because a parser broke — reextract.py replays it instead.
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
import uuid
from datetime import datetime

import mq
from ad_extractor import extract_ads
from db.jobs import JobStore
from db.store import read_html

_STOP = False


def _handle_signal(signum, frame):
    global _STOP
    _STOP = True
    print("\nFinishing current message, then exiting…", file=sys.stderr)


class Extractor:
    def __init__(self, args):
        self.args = args
        self.store = JobStore(args.dsn)
        self.counts: dict[str, int] = {}
        self.processed = 0

    def close(self):
        self.store.close()

    def _bump(self, key: str) -> None:
        self.counts[key] = self.counts.get(key, 0) + 1

    def handle(self, channel, method, properties, body) -> None:
        global _STOP
        try:
            o = json.loads(body)
        except Exception as exc:
            print(f"  unparseable outcome, dropping: {exc}", file=sys.stderr)
            channel.basic_ack(method.delivery_tag)
            return

        request_id = uuid.UUID(o["request_id"])
        job_id = uuid.UUID(o["job_id"]) if o.get("job_id") else None

        try:
            self.store.record_attempt_raw(o)
        except Exception as exc:
            # A failed audit write must not drop the message; requeue for another go.
            print(f"  audit insert failed for {request_id}: {exc}", file=sys.stderr)
            channel.basic_nack(method.delivery_tag, requeue=True)
            self._bump("audit_failed")
            return

        if o["status"] in ("ok", "empty_serp") and o.get("html_path"):
            try:
                ads = extract_ads(read_html(o["html_path"]), query=o["term"])
                self.store.record_serp(request_id, ads)
                self._bump("extracted")
            except FileNotFoundError:
                self._bump("html_missing")
                print(f"  html missing for {request_id}: {o['html_path']}", file=sys.stderr)
            except Exception as exc:
                self._bump("parse_failed")
                print(f"  parse failed for {request_id}: {exc}", file=sys.stderr)
        else:
            self._bump(o["status"])

        if job_id and o["status"] in ("ok", "empty_serp"):
            self.store.mark_extracted(job_id)

        channel.basic_ack(method.delivery_tag)
        self.processed += 1

        if self.processed % self.args.progress_every == 0:
            summary = " ".join(f"{k}={v}" for k, v in sorted(self.counts.items()))
            print(f"[extractor] {self.processed} done | {summary}", flush=True)

        if _STOP or (self.args.max_jobs and self.processed >= self.args.max_jobs):
            channel.stop_consuming()


def main() -> int:
    ap = argparse.ArgumentParser(description="Consume scrape outcomes, extract ads into Postgres.")
    ap.add_argument("--amqp-url", default=mq.DEFAULT_URL)
    ap.add_argument("--dsn", default=None)
    ap.add_argument("--once", action="store_true", help="Exit when the queue is empty.")
    ap.add_argument("--max-jobs", type=int, default=None)
    ap.add_argument("--prefetch", type=int, default=10)
    ap.add_argument("--progress-every", type=int, default=25)
    args = ap.parse_args()

    if args.dsn is None:
        from db.store import DEFAULT_DSN
        args.dsn = DEFAULT_DSN

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn = mq.connect(args.amqp_url)
    channel = conn.channel()
    mq.declare_topology(channel)
    channel.basic_qos(prefetch_count=args.prefetch)

    ex = Extractor(args)
    print(f"[extractor] waiting on {mq.EXTRACT_QUEUE}", flush=True)
    try:
        if args.once:
            while not _STOP:
                method, properties, body = channel.basic_get(mq.EXTRACT_QUEUE, auto_ack=False)
                if method is None:
                    break
                ex.handle(channel, method, properties, body)
                if args.max_jobs and ex.processed >= args.max_jobs:
                    break
        else:
            channel.basic_consume(mq.EXTRACT_QUEUE, ex.handle, auto_ack=False)
            channel.start_consuming()
    finally:
        ex.close()
        try:
            conn.close()
        except Exception:
            pass

    summary = " ".join(f"{k}={v}" for k, v in sorted(ex.counts.items()))
    print(f"[extractor] exited after {ex.processed} message(s) | {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
