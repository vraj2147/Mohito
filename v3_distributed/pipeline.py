"""Run the whole v3 pipeline as one job.

Loads work, starts N scraper workers and an extractor, supervises them until the
batch reaches a terminal state, then prints the full report. The three stages stay
separate processes — this only orchestrates them, so anything it does by hand can
still be done by running the stages individually.

    python v3_distributed/pipeline.py --distinct --limit 50 --workers 2
    python v3_distributed/pipeline.py --term "iPhone 16 Pro" --limit 100
    python v3_distributed/pipeline.py --resume --workers 2

Supervision, not just spawning:

* reclaims jobs whose worker died holding a lease, on a timer
* republishes anything left `pending`/`retry` that no longer has a live message
* restarts a worker process that exits unexpectedly while work remains
* shuts everything down on SIGINT, waiting for in-flight scrapes to finish

Completion is judged from the ledger — every job terminal (`extracted`/`dead`) and
both queues drained — not from process exit, because a worker can exit while a
retry is still parked in the TTL queue.
"""

from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import mq
from db.jobs import JobStore

_STOP = False
PY = sys.executable
V3 = Path(__file__).resolve().parent


def _handle_sigint(signum, frame):
    global _STOP
    if _STOP:
        raise KeyboardInterrupt
    _STOP = True
    print("\n[pipeline] shutting down — waiting for in-flight scrapes…", file=sys.stderr)


class Supervisor:
    def __init__(self, args):
        self.args = args
        self.procs: dict[str, subprocess.Popen] = {}
        self.log_dir = Path(args.log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def spawn(self, name: str, cmd: list[str]) -> None:
        log = (self.log_dir / f"{name}.log").open("a")
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        self.procs[name] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
        print(f"[pipeline] started {name} (pid {self.procs[name].pid}) -> {self.log_dir/name}.log")

    def start_workers(self) -> None:
        for i in range(1, self.args.workers + 1):
            wid = f"w{i}"
            cmd = [PY, str(V3 / "scraper_worker.py"), "--worker-id", wid,
                   "--out-dir", self.args.out_dir,
                   "--min-delay", str(self.args.min_delay),
                   "--max-delay", str(self.args.max_delay),
                   "--lease-seconds", str(self.args.lease_seconds)]
            if self.args.headed:
                cmd.append("--headed")
            else:
                # Spawned workers have no window server, so a headed escalation
                # would hang. They back off and stay headless instead.
                cmd.append("--no-escalate")
            self.spawn(wid, cmd)
        self.spawn("extractor", [PY, str(V3 / "extractor_worker.py")])

    def kill_worker(self, name: str) -> None:
        p = self.procs.get(name)
        if p and p.poll() is None:
            p.kill()
            p.wait(timeout=30)

    def restart_dead(self) -> None:
        for name, p in list(self.procs.items()):
            if p.poll() is None:
                continue
            print(f"[pipeline] {name} exited (rc={p.returncode}) — restarting", file=sys.stderr)
            if name == "extractor":
                self.spawn(name, [PY, str(V3 / "extractor_worker.py")])
            else:
                cmd = [PY, str(V3 / "scraper_worker.py"), "--worker-id", name,
                       "--out-dir", self.args.out_dir,
                       "--min-delay", str(self.args.min_delay),
                       "--max-delay", str(self.args.max_delay),
                       "--lease-seconds", str(self.args.lease_seconds)]
                if self.args.headed:
                    cmd.append("--headed")
                else:
                    cmd.append("--no-escalate")
                self.spawn(name, cmd)

    def shutdown(self, grace: int = 60) -> None:
        for name, p in self.procs.items():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        deadline = time.time() + grace
        for name, p in self.procs.items():
            remaining = max(1, int(deadline - time.time()))
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                print(f"[pipeline] {name} did not stop in time — killing", file=sys.stderr)
                p.kill()


def run_loader(args) -> int:
    cmd = [PY, str(V3 / "loader.py")]
    if args.resume:
        cmd.append("--resume")
    else:
        if args.term:
            cmd += ["--term", args.term]
        if args.distinct:
            cmd.append("--distinct")
        if args.limit:
            cmd += ["--limit", str(args.limit)]
        cmd += ["--max-attempts", str(args.max_attempts)]
        if args.notes:
            cmd += ["--notes", args.notes]
    print(f"[pipeline] loading work: {' '.join(cmd[2:])}")
    return subprocess.run(cmd).returncode


def report(store: JobStore, batch_id: int | None) -> None:
    print("\n" + "=" * 78)
    print("PIPELINE REPORT")
    print("=" * 78)

    rows = store.progress(batch_id)
    print("\nJOB LEDGER (scrape_jobs)")
    print("-" * 78)
    print(f"{'batch':>6}{'loaded':>8}{'extracted':>11}{'left':>7}{'in_flight':>11}"
          f"{'to_extract':>12}{'dead':>7}{'pct':>7}")
    print("-" * 78)
    for r in rows:
        print(f"{r['batch_id']:>6}{r['total']:>8}{r['done']:>11}{r['waiting']:>7}"
              f"{r['in_flight']:>11}{r['awaiting_extract']:>12}{r['dead']:>7}{r['pct_done']:>6}%")

    where = "WHERE batch_id = %s" if batch_id else ""
    params = (batch_id,) if batch_id else ()

    print("\nATTEMPTS (scrape_requests)")
    print("-" * 78)
    for r in store.query(
        f"SELECT status, count(*) n, round(avg(duration_ms)) avg_ms, "
        f"count(DISTINCT worker_id) workers FROM scrape_requests {where} "
        f"GROUP BY status ORDER BY n DESC", params):
        print(f"  {r['status']:<16}{r['n']:>7}  avg {str(r['avg_ms'] or '-'):>7}ms  "
              f"{r['workers']} worker(s)")

    tot = store.query(
        f"SELECT count(*) attempts, count(DISTINCT term) terms, "
        f"sum(html_bytes) bytes FROM scrape_requests {where}", params)[0]
    if tot["attempts"]:
        print(f"  {'total':<16}{tot['attempts']:>7}  {tot['terms']} distinct terms"
              f"  {(tot['bytes'] or 0)/1e6:.0f} MB html")

    print("\nEXTRACTED (serp_results / serp_ads)")
    print("-" * 78)
    j = "JOIN scrape_requests q USING (request_id)"
    w = "WHERE q.batch_id = %s" if batch_id else ""
    r = store.query(
        f"SELECT count(*) serps, round(avg(v.sponsored_result_count),2) avg_res, "
        f"round(avg(v.sponsored_product_count),2) avg_prod, "
        f"round(avg(v.organic_count),2) avg_org FROM serp_results v {j} {w}", params)[0]
    print(f"  SERPs parsed        {r['serps'] or 0}")
    print(f"  avg sponsored results  {r['avg_res'] or 0}")
    print(f"  avg sponsored products {r['avg_prod'] or 0}")
    print(f"  avg organic            {r['avg_org'] or 0}")

    for row in store.query(
        f"SELECT a.ad_type, count(*) n FROM serp_ads a {j} {w} GROUP BY 1 ORDER BY 2 DESC", params):
        print(f"  {row['ad_type']:<22}{row['n']:>7} rows")

    dead = store.query(
        f"SELECT term, attempts, last_status, left(coalesce(last_error,''),50) err "
        f"FROM scrape_jobs WHERE status='dead'"
        + (" AND batch_id = %s" if batch_id else "") + " LIMIT 10", params)
    if dead:
        print("\nDEAD JOBS (exhausted attempts)")
        print("-" * 78)
        for d in dead:
            print(f"  {d['term'][:34]:<36}{d['attempts']:>3}x  {d['last_status']}  {d['err']}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run loader + scrapers + extractor as one job.")
    ap.add_argument("--workers", type=int, default=1, help="Scraper worker processes.")
    ap.add_argument("--term", default=None)
    ap.add_argument("--distinct", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--resume", action="store_true", help="Republish pending work instead of a new batch.")
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--notes", default=None)
    ap.add_argument("--out-dir", default="runs/mq/html")
    ap.add_argument("--log-dir", default="runs/mq/logs")
    ap.add_argument("--min-delay", type=float, default=1.0)
    ap.add_argument("--max-delay", type=float, default=3.0)
    ap.add_argument("--lease-seconds", type=int, default=300)
    ap.add_argument("--reclaim-every", type=int, default=60, help="Seconds between lease sweeps.")
    ap.add_argument("--poll-every", type=int, default=10)
    ap.add_argument("--stall-seconds", type=int, default=180,
                    help="Restart a worker that has recorded no attempt for this long.")
    ap.add_argument("--timeout", type=int, default=0, help="Give up after N seconds (0 = no limit).")
    ap.add_argument("--headed", action="store_true")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    store = JobStore()
    if run_loader(args) != 0:
        print("[pipeline] loader failed — nothing to do.", file=sys.stderr)
        store.close()
        return 1
    batch_id = store.latest_batch()

    conn = mq.connect()
    channel = conn.channel()
    mq.declare_topology(channel)

    sup = Supervisor(args)
    sup.start_workers()

    started = time.time()
    last_reclaim = 0.0
    try:
        while not _STOP:
            time.sleep(args.poll_every)

            if time.time() - last_reclaim > args.reclaim_every:
                reclaimed = store.reclaim_expired()
                if reclaimed:
                    retryable = [r for r in reclaimed if r["status"] == "retry"]
                    print(f"[pipeline] reclaimed {len(reclaimed)} expired "
                          f"({len(retryable)} requeued)", file=sys.stderr)
                    for r in retryable:
                        mq.publish(channel, mq.JOBS_QUEUE, {
                            "job_id": str(r["job_id"]), "batch_id": r["batch_id"],
                            "term_id": r["term_id"], "term": r["term"],
                            "locale": r.get("locale", "en-GB"),
                        }, message_id=str(r["job_id"]))
                    store.mark_published([r["job_id"] for r in retryable])
                last_reclaim = time.time()

            sup.restart_dead()

            # A worker can hang while still alive — a browser launch that never
            # returns leaves the process healthy-looking but idle forever. Process
            # liveness alone will not catch it, so progress is judged from the
            # ledger and a silent worker is killed and respawned.
            silent = store.query(
                """SELECT w.worker_id,
                          round(extract(epoch FROM now() - max(q.started_at))) AS quiet_for
                   FROM (SELECT DISTINCT worker_id FROM scrape_requests
                         WHERE batch_id = %s AND worker_id IS NOT NULL) w
                   JOIN scrape_requests q ON q.worker_id = w.worker_id AND q.batch_id = %s
                   GROUP BY w.worker_id""", (batch_id, batch_id))
            for row in silent:
                wid = row["worker_id"]
                if wid in sup.procs and row["quiet_for"] and row["quiet_for"] > args.stall_seconds:
                    print(f"[pipeline] {wid} silent for {int(row['quiet_for'])}s — "
                          f"restarting (stalled)", file=sys.stderr)
                    sup.kill_worker(wid)
                    sup.restart_dead()

            p = store.progress(batch_id)[0]
            d = mq.depths(channel)
            outstanding = int(p["waiting"]) + int(p["in_flight"]) + int(p["awaiting_extract"])
            print(f"[pipeline] {p['done']}/{p['total']} extracted | left={p['waiting']} "
                  f"in_flight={p['in_flight']} to_extract={p['awaiting_extract']} "
                  f"dead={p['dead']} | q={d[mq.JOBS_QUEUE]}/{d[mq.RETRY_QUEUE]}/{d[mq.EXTRACT_QUEUE]}",
                  flush=True)

            if outstanding == 0 and d[mq.JOBS_QUEUE] == 0 and d[mq.RETRY_QUEUE] == 0 \
                    and d[mq.EXTRACT_QUEUE] == 0:
                print("[pipeline] batch complete.")
                break

            if args.timeout and time.time() - started > args.timeout:
                print(f"[pipeline] timeout after {args.timeout}s — stopping.", file=sys.stderr)
                break
    finally:
        sup.shutdown()
        report(store, batch_id)
        store.close()
        try:
            conn.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
