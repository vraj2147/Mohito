"""Re-parse saved SERP HTML back into the database.

The raw pages on disk are the durable artefact; serp_results / serp_ads are just a
derived view of them. When the extractor improves — or Google changes its markup —
this replays the saved HTML instead of re-scraping, which costs nothing and cannot
be blocked.

    python reextract.py --run 8 9
    python reextract.py --all --dry-run
"""


from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import sys

from ad_extractor import extract_ads
from db.store import DEFAULT_DSN, Store, read_html


def main() -> int:
    ap = argparse.ArgumentParser(description="Re-parse stored HTML into serp_results/serp_ads.")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--run", type=int, nargs="+", help="Run id(s) to re-extract.")
    ap.add_argument("--all", action="store_true", help="Every successful scrape on record.")
    ap.add_argument("--dry-run", action="store_true", help="Report differences without writing.")
    args = ap.parse_args()

    if not args.run and not args.all:
        ap.error("pass --run <id...> or --all")

    with Store(args.dsn) as store:
        sql = ("SELECT request_id, term, html_path FROM scrape_requests "
               "WHERE status = 'ok' AND html_path IS NOT NULL")
        params: tuple = ()
        if args.run:
            sql += " AND run_id = ANY(%s)"
            params = (args.run,)
        rows = store.query(sql, params)

        if not rows:
            print("Nothing to re-extract.", file=sys.stderr)
            return 1
        print(f"Re-extracting {len(rows)} saved pages…")

        changed = missing = failed = 0
        for i, r in enumerate(rows, 1):
            try:
                ads = extract_ads(read_html(r["html_path"]), query=r["term"])
            except FileNotFoundError:
                missing += 1
                continue
            except Exception as exc:
                failed += 1
                print(f"  parse failed {r['request_id']}: {exc}", file=sys.stderr)
                continue

            before = store.query(
                "SELECT sponsored_result_count, sponsored_product_count FROM serp_results WHERE request_id = %s",
                (r["request_id"],),
            )
            if not before or (before[0]["sponsored_result_count"], before[0]["sponsored_product_count"]) != (
                ads.sponsored_result_count, ads.sponsored_product_count
            ):
                changed += 1

            if not args.dry_run:
                # serp_ads cascades from serp_results, so clearing the parent row
                # drops its ads too and the re-insert starts from a clean slate.
                store.execute("DELETE FROM serp_results WHERE request_id = %s", (r["request_id"],))
                store.execute("DELETE FROM serp_ads     WHERE request_id = %s", (r["request_id"],))
                store.record_serp(r["request_id"], ads)

            if i % 50 == 0:
                print(f"  [{i}/{len(rows)}]", flush=True)

        verb = "would change" if args.dry_run else "changed"
        print(f"\ndone: {len(rows)} pages, {verb} {changed} counts, "
              f"{missing} html missing, {failed} parse failures")
    return 0


if __name__ == "__main__":
    sys.exit(main())
