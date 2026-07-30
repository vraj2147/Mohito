"""Scrape metrics and failure forensics.

Answers the two questions the audit log exists for: how healthy was a run, and
exactly which searches failed and why. Every failed attempt is addressable by its
request_id, so a failure can be traced to its term, its timing and — when a page
was retrieved at all — its raw HTML on disk.

    python metrics.py                      # latest run
    python metrics.py --run 2              # a specific run
    python metrics.py --errors             # per-request failure list
    python metrics.py --errors --status captcha
    python metrics.py --ads                # sponsored rates by intent and term
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
    print("\nSPONSORED RATE BY INTENT")
    print("-" * 78)
    print(f"{'intent':<16}{'serps':>7}{'sp_res%':>9}{'sp_prod%':>10}{'res/serp':>10}{'prod/serp':>11}")
    print("-" * 78)
    for r in store.query(
        """
        SELECT COALESCE(t.intent, 'unknown')                                   AS intent,
               count(*)                                                        AS serps,
               round(100.0 * count(*) FILTER (WHERE v.sponsored_result_count > 0) / count(*), 1) AS sp_res_pct,
               round(100.0 * count(*) FILTER (WHERE v.sponsored_product_count      > 0) / count(*), 1) AS sp_prod_pct,
               round(avg(v.sponsored_result_count), 2)                                  AS avg_res,
               round(avg(v.sponsored_product_count), 2)                                      AS avg_prod
        FROM   scrape_requests q
        JOIN   serp_results  v ON v.request_id = q.request_id
        LEFT   JOIN search_terms t ON t.id = q.term_id
        WHERE  q.run_id = %s AND q.status = 'ok'
        GROUP  BY 1 ORDER BY serps DESC
        """,
        (run_id,),
    ):
        print(
            f"{r['intent']:<16}{r['serps']:>7}{r['sp_res_pct']:>9}{r['sp_prod_pct']:>10}"
            f"{r['avg_res']:>10}{r['avg_prod']:>11}"
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
        print(f"  {r['ad_type']:<20}{r['advertiser'][:40]:<42}{r['n']:>7}")


def compare_runs(store: Store, run_ids: list[int]) -> None:
    """Put runs side by side. Built for the breadth-vs-repeat comparison: a batch
    of distinct terms and a batch repeating one term stress different things —
    breadth samples many SERP shapes, repeats measure how stable one SERP is."""
    rows = store.query(
        """
        SELECT q.run_id,
               s.notes,
               count(*)                                            AS attempts,
               count(*) FILTER (WHERE q.status = 'ok')             AS ok,
               count(*) FILTER (WHERE q.status = 'captcha')        AS captcha,
               count(DISTINCT q.term)                              AS terms,
               round(avg(q.duration_ms) FILTER (WHERE q.status='ok'))  AS avg_ms,
               count(*) FILTER (WHERE q.status='ok' AND NOT q.headless) AS headed_ok,
               round(avg(v.sponsored_result_count), 2)                      AS avg_sponsored_results,
               round(avg(v.sponsored_product_count), 2)                          AS avg_sponsored_products,
               round(100.0 * count(*) FILTER (WHERE v.sponsored_result_count > 0)
                     / NULLIF(count(v.request_id), 0), 1)          AS sponsored_result_rate,
               round(100.0 * count(*) FILTER (WHERE v.sponsored_product_count > 0)
                     / NULLIF(count(v.request_id), 0), 1)          AS sponsored_product_rate,
               round(stddev_samp(v.sponsored_product_count), 2)                  AS sponsored_product_stddev,
               round(stddev_samp(v.sponsored_result_count), 2)              AS sponsored_result_stddev
        FROM   scrape_requests q
        JOIN   scrape_runs s ON s.id = q.run_id
        LEFT   JOIN serp_results v ON v.request_id = q.request_id
        WHERE  q.run_id = ANY(%s)
        GROUP  BY q.run_id, s.notes ORDER BY q.run_id
        """,
        (run_ids,),
    )
    if not rows:
        print("No matching runs.", file=sys.stderr)
        return

    labels = [f"run {r['run_id']}" for r in rows]
    print("=" * (26 + 18 * len(rows)))
    print("RUN COMPARISON")
    print("=" * (26 + 18 * len(rows)))
    print(f"{'':<26}" + "".join(f"{l:>18}" for l in labels))
    print("-" * (26 + 18 * len(rows)))

    def line(label, key, fmt="{}"):
        print(f"{label:<26}" + "".join(
            f"{(fmt.format(r[key]) if r[key] is not None else '-'):>18}" for r in rows
        ))

    for r in rows:
        pass
    print(f"{'notes':<26}" + "".join(f"{(r['notes'] or '')[:17]:>18}" for r in rows))
    line("distinct terms", "terms")
    line("attempts", "attempts")
    line("ok", "ok")
    line("captcha", "captcha")
    line("ok via headed", "headed_ok")
    line("avg ok duration (ms)", "avg_ms")
    print("-" * (26 + 18 * len(rows)))
    line("sponsored result rate %", "sponsored_result_rate")
    line("sponsored product rate %", "sponsored_product_rate")
    line("avg sponsored results/SERP", "avg_sponsored_results")
    line("avg sponsored products/SERP", "avg_sponsored_products")
    line("sponsored result stddev", "sponsored_result_stddev")
    line("sponsored product stddev", "sponsored_product_stddev")


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
    ap.add_argument("--compare", type=int, nargs="+", metavar="RUN_ID",
                    help="Compare two or more runs side by side.")
    args = ap.parse_args()

    with Store(args.dsn) as store:
        if args.compare:
            compare_runs(store, args.compare)
            return 0
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
