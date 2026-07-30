"""Job 1 of 3: load work into RabbitMQ.

Creates a batch, writes one ledger row per unit of work, then publishes each to
`scrape.jobs`. Rows are written BEFORE publishing and only flipped to `queued`
once the broker confirms, so a crash mid-load leaves `pending` rows that
`--resume` republishes rather than work that silently vanished.

    python loader.py --distinct 50                     # one scrape per pending term
    python loader.py --term "iPhone 16 Pro" --limit 50 # repeats of one term
    python loader.py --resume                          # republish pending/retry
    python loader.py --status                          # ledger + queue depths
    python loader.py --reclaim                         # recover dead workers' jobs
"""


from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import sys

import mq
from db.jobs import JobStore


def show_status(store: JobStore, channel) -> None:
    print("QUEUE DEPTHS")
    print("-" * 58)
    for q, n in mq.depths(channel).items():
        print(f"  {q:<22}{n:>8}")

    rows = store.progress()
    if not rows:
        print("\nNo batches yet.")
        return
    print("\nLEDGER")
    print("-" * 100)
    print(f"{'batch':>6}{'total':>8}{'done':>8}{'waiting':>9}{'in_flight':>11}"
          f"{'to_extract':>12}{'dead':>7}{'pct':>7}  notes")
    print("-" * 100)
    for r in rows:
        print(f"{r['batch_id']:>6}{r['total']:>8}{r['done']:>8}{r['waiting']:>9}"
              f"{r['in_flight']:>11}{r['awaiting_extract']:>12}{r['dead']:>7}"
              f"{r['pct_done']:>6}%  {(r['notes'] or '')[:32]}")

    stuck = store.query("SELECT count(*) n FROM v_stuck_jobs")[0]["n"]
    if stuck:
        print(f"\n  {stuck} job(s) past their lease — run --reclaim to requeue them.")


def publish_jobs(store: JobStore, channel, rows: list[dict]) -> int:
    """Publish rows and mark them queued only once the broker has confirmed."""
    published: list = []
    for r in rows:
        try:
            mq.publish(
                channel, mq.JOBS_QUEUE,
                {
                    "job_id": str(r["job_id"]),
                    "batch_id": r.get("batch_id"),
                    "term_id": r.get("term_id"),
                    "term": r["term"],
                    "locale": r.get("locale", "en-GB"),
                },
                message_id=str(r["job_id"]),
            )
            published.append(r["job_id"])
        except Exception as exc:
            print(f"publish failed for {r['job_id']}: {exc}", file=sys.stderr)
            break
    if published:
        store.mark_published(published)
    return len(published)


def main() -> int:
    ap = argparse.ArgumentParser(description="Load scrape work into RabbitMQ.")
    ap.add_argument("--amqp-url", default=mq.DEFAULT_URL)
    ap.add_argument("--term", default=None, help="Restrict to one search term.")
    ap.add_argument("--distinct", action="store_true", help="At most one job per term.")
    ap.add_argument("--limit", type=int, default=None, help="Cap the number of jobs.")
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--no-shuffle", action="store_true")
    ap.add_argument("--notes", default=None)
    ap.add_argument("--resume", action="store_true",
                    help="Republish pending/retry jobs instead of creating a batch.")
    ap.add_argument("--reclaim", action="store_true",
                    help="Requeue jobs whose worker died holding the lease.")
    ap.add_argument("--status", action="store_true", help="Show ledger and queue depths.")
    ap.add_argument("--retry-ttl-ms", type=int, default=mq.DEFAULT_RETRY_TTL_MS)
    args = ap.parse_args()

    conn = mq.connect(args.amqp_url)
    channel = conn.channel()
    mq.declare_topology(channel, retry_ttl_ms=args.retry_ttl_ms)
    channel.confirm_delivery()

    store = JobStore()
    try:
        if args.status:
            show_status(store, channel)
            return 0

        if args.reclaim:
            rows = store.reclaim_expired()
            retryable = [r for r in rows if r["status"] == "retry"]
            dead = len(rows) - len(retryable)
            print(f"Reclaimed {len(rows)} expired job(s): {len(retryable)} requeued, {dead} dead.")
            if retryable:
                publish_jobs(store, channel, retryable)
            return 0

        if args.resume:
            rows = store.unpublished()
            if not rows:
                print("Nothing pending to republish.")
                return 0
            n = publish_jobs(store, channel, rows)
            print(f"Republished {n}/{len(rows)} job(s).")
            return 0

        jobs = store.build_queue(
            shuffle=not args.no_shuffle, cap=args.limit,
            term=args.term, distinct=args.distinct,
        )
        if not jobs:
            print("Queue is empty — nothing pending in search_terms.", file=sys.stderr)
            return 1

        batch_id, batch_uuid = store.create_batch(config=vars(args), notes=args.notes)
        rows = store.add_jobs(batch_id, jobs, max_attempts=args.max_attempts)
        for r in rows:
            r["batch_id"] = batch_id
        n = publish_jobs(store, channel, rows)

        print(f"Batch {batch_id} ({batch_uuid})")
        print(f"  ledger rows : {len(rows)}")
        print(f"  published   : {n}")
        print(f"  queue depth : {mq.queue_depth(channel, mq.JOBS_QUEUE)}")
        if n < len(rows):
            print(f"  {len(rows) - n} row(s) left pending — rerun with --resume.", file=sys.stderr)
        return 0
    finally:
        store.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
