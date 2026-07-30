"""Scrape metrics and failure forensics.

Answers the two questions the audit log exists for: how healthy was a run, and
exactly which searches failed and why. Every failed attempt is addressable by its
request_id, so a failure can be traced to its term, its timing and — when a page
was retrieved at all — its raw HTML on disk.

    python metrics.py                      # latest run
    python metrics.py --run 2              # a specific run
    python metrics.py --errors             # per-request failure list
    python metrics.py --errors --status captcha
    python metrics.py --ads                # ad rates by intent and term
    python metrics.py --html <request_id>  # dump that request's raw HTML
"""

from __future__ import annotations

import argparse
import sys

from db.store import DEFAULT_DSN, Store


def latest_run(store: Store) -> int | None:
    rows = store.query("SELECT id FROM scrape_runs ORDER BY id DESC LIMIT 1")
    return rows[0]["id"] if rows else None


def show_run(store: Store, run_id: int) -> None:
    rows = store.query("SELECT * FROM v_run_summary WHERE run_id = %s", (run_id,))
    if not rows:
        print(f"No run {run_id}.", file=sys.stderr)
        return
    r = rows[0]
    attempts = r["attempts"] or 0
    ok = r["ok"] or 0

    print("=" * 66)
    print(f"RUN {run_id}  {r['run_uuid']}")
    print("=" * 66)
    print(f"started        : {r['started_at']:%Y-%m-%d %H:%M:%S}")
    print(f"finished       : {r['finished_at'] or '(still running)'}")
    print(f"attempts       : {attempts}")
    print(f"ok             : {ok}" + (f"  ({ok / attempts:.1%})" if attempts else ""))
    print(f"captcha        : {r['captcha']}")
    print(f"other errors   : {r['other_errors']}")
    if r["avg_ok_ms"]:
        print(f"avg ok time    : {int(r['avg_ok_ms'])} ms")
    if r["html_bytes"]:
        print(f"html captured  : {r['html_bytes'] / 1e9:.2f} GB (uncompressed)")

    print("\nSTATUS BREAKDOWN")
    print("-" * 66)
    for row in store.query(
        "SELECT status, n, pct FROM v_error_metrics WHERE run_id = %s ORDER BY n DESC",
        (run_id,),
    ):
        print(f"  {row['status']:<18}{row['n']:>7}  {row['pct']:>6}%")

    # Retries are only visible if attempt > 1 rows exist.
    retried = store.query(
        "SELECT count(*) n FROM scrape_requests WHERE run_id = %s AND attempt > 1",
        (run_id,),
    )[0]["n"]
    if retried:
        print(f"\n  retried attempts : {retried}")


def show_errors(store: Store, run_id: int, status: str | None, limit: int) -> None:
    sql = """
        SELECT request_id, term, status, error_class, attempt,
               duration_ms, started_at, html_path,
               left(coalesce(error_message, ''), 90) AS msg
        FROM   scrape_requests
        WHERE  run_id = %s AND status <> 'ok'
    """
    params: list = [run_id]
    if status:
        sql += " AND status = %s"
        params.append(status)
    sql += " ORDER BY started_at DESC LIMIT %s"
    params.append(limit)

    rows = store.query(sql, tuple(params))
    if not rows:
        print("No failed attempts recorded for this run.")
        return

    print(f"\nFAILED ATTEMPTS ({len(rows)} shown)")
    print("-" * 108)
    print(f"{'request_id':<38}{'status':<15}{'try':>4}  {'term':<30}error")
    print("-" * 108)
    for r in rows:
        print(
            f"{str(r['request_id']):<38}{r['status']:<15}{r['attempt']:>4}  "
            f"{r['term'][:29]:<30}{(r['error_class'] or '')} {r['msg']}".rstrip()
        )

    print("\nFAILURES BY TERM (worst first)")
    print("-" * 66)
    for r in store.query(
        """SELECT term, count(*) fails, string_agg(DISTINCT status, ',') statuses
           FROM   scrape_requests
           WHERE  run_id = %s AND status <> 'ok'
           GROUP  BY term ORDER BY fails DESC LIMIT 15""",
        (run_id,),
    ):
        print(f"  {r['fails']:>4}  {r['statuses']:<26}{r['term'][:32]}")


def show_ads(store: Store, run_id: int) -> None:
    print("\nAD RATE BY INTENT")
    print("-" * 78)
    print(f"{'intent':<16}{'serps':>7}{'text%':>8}{'pla%':>8}{'txt/serp':>10}{'pla/serp':>10}")
    print("-" * 78)
    for r in store.query(
        """
        SELECT COALESCE(t.intent, 'unknown')                                   AS intent,
               count(*)                                                        AS serps,
               round(100.0 * count(*) FILTER (WHERE v.text_ad_count > 0) / count(*), 1) AS text_pct,
               round(100.0 * count(*) FILTER (WHERE v.pla_count      > 0) / count(*), 1) AS pla_pct,
               round(avg(v.text_ad_count), 2)                                  AS avg_text,
               round(avg(v.pla_count), 2)                                      AS avg_pla
        FROM   scrape_requests q
        JOIN   serp_results  v ON v.request_id = q.request_id
        LEFT   JOIN search_terms t ON t.id = q.term_id
        WHERE  q.run_id = %s AND q.status = 'ok'
        GROUP  BY 1 ORDER BY serps DESC
        """,
        (run_id,),
    ):
        print(
            f"{r['intent']:<16}{r['serps']:>7}{r['text_pct']:>8}{r['pla_pct']:>8}"
            f"{r['avg_text']:>10}{r['avg_pla']:>10}"
        )

    print("\nTOP ADVERTISERS / MERCHANTS")
    print("-" * 78)
    for r in store.query(
        """SELECT a.ad_type, a.advertiser, count(*) n
           FROM   serp_ads a JOIN scrape_requests q ON q.request_id = a.request_id
           WHERE  q.run_id = %s AND a.advertiser IS NOT NULL AND a.advertiser <> ''
           GROUP  BY 1, 2 ORDER BY n DESC LIMIT 15""",
        (run_id,),
    ):
        print(f"  {r['ad_type']:<6}{r['advertiser'][:46]:<48}{r['n']:>7}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape run metrics and failure forensics.")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--run", type=int, default=None, help="Run id (default: latest).")
    ap.add_argument("--errors", action="store_true", help="List failed attempts.")
    ap.add_argument("--status", default=None, help="Filter --errors to one status.")
    ap.add_argument("--ads", action="store_true", help="Show ad rates by intent.")
    ap.add_argument("--limit", type=int, default=40, help="Rows for --errors.")
    ap.add_argument("--html", default=None, metavar="REQUEST_ID",
                    help="Print the raw HTML stored for one request_id.")
    args = ap.parse_args()

    with Store(args.dsn) as store:
        if args.html:
            html = store.get_html(args.html)
            if html is None:
                print(f"No HTML recorded for {args.html}", file=sys.stderr)
                return 1
            try:
                sys.stdout.write(html)
            except BrokenPipeError:
                # Normal when piping into head/less: the reader closed early.
                return 0
            return 0

        run_id = args.run or latest_run(store)
        if run_id is None:
            print("No runs recorded yet.", file=sys.stderr)
            return 1

        show_run(store, run_id)
        if args.errors:
            show_errors(store, run_id, args.status, args.limit)
        if args.ads:
            show_ads(store, run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
