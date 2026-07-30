"""Diagnose CAPTCHAs and environment problems.

Run this first when scrapes start getting blocked. It reports the things that
actually determine whether Google serves results or a challenge, rather than
guessing.

    python doctor.py                 # full check
    python doctor.py --captcha       # only the blocking analysis
    python doctor.py --open-captcha  # dump the latest saved CAPTCHA page
"""

from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import glob
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Cookies Google sets for an established session. NID is the meaningful one: a
# profile without it looks brand new, and brand-new profiles get challenged on
# their first request.
WARM_COOKIES = ("NID", "SOCS", "AEC", "__Secure-ENID", "CONSENT")
KEY_COOKIE = "NID"


def ok(msg): print(f"  \033[32m✓\033[0m {msg}")
def warn(msg): print(f"  \033[33m!\033[0m {msg}")
def bad(msg): print(f"  \033[31m✗\033[0m {msg}")


def check_services() -> None:
    print("\nSERVICES")
    print("-" * 62)
    try:
        from db.store import Store
        s = Store()
        n = s.query("SELECT count(*) n FROM search_terms")[0]["n"]
        ok(f"postgres reachable — {n} search terms")
        s.close()
    except Exception as exc:
        bad(f"postgres: {exc}")

    try:
        import mq
        conn = mq.connect()
        ch = conn.channel()
        mq.declare_topology(ch)
        d = mq.depths(ch)
        ok(f"rabbitmq reachable — jobs={d[mq.JOBS_QUEUE]} retry={d[mq.RETRY_QUEUE]} "
           f"extract={d[mq.EXTRACT_QUEUE]} dead={d[mq.DEAD_QUEUE]}")
        if d[mq.JOBS_QUEUE] > 500:
            warn(f"{d[mq.JOBS_QUEUE]} jobs queued — the queue is FIFO, so a new "
                 f"batch will sit behind this backlog")
        conn.close()
    except Exception as exc:
        bad(f"rabbitmq: {exc}")

    try:
        out = subprocess.run(["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                              "--version"], capture_output=True, text=True, timeout=10)
        ok(f"chrome: {out.stdout.strip()}")
    except Exception:
        warn("real Chrome not found — Playwright will fall back to bundled Chromium")


def profile_report() -> None:
    """Cookie warmth per profile — the strongest predictor of being challenged."""
    print("\nPROFILE WARMTH")
    print("-" * 62)
    profiles = sorted(glob.glob(str(Path.home() / ".cache" / "google_scraper*")))
    if not profiles:
        warn("no profiles yet — the first run will be cold and will be challenged")
        return

    print(f"  {'profile':<34}{'google cookies':>15}{'NID':>6}")
    for p in profiles:
        cookie_db = Path(p) / "Default" / "Cookies"
        name = Path(p).name
        if not cookie_db.exists():
            print(f"  {name:<34}{'(no cookie db)':>15}{'-':>6}")
            continue
        try:
            c = sqlite3.connect(f"file:{cookie_db}?immutable=1", uri=True)
            rows = c.execute(
                "SELECT name FROM cookies WHERE host_key LIKE '%google%'").fetchall()
            c.close()
        except Exception as exc:
            print(f"  {name:<34}{'(locked/in use)':>15}{'-':>6}")
            continue
        names = {n for (n,) in rows}
        has_nid = KEY_COOKIE in names
        print(f"  {name:<34}{len(rows):>15}{'yes' if has_nid else 'NO':>6}")

    print()
    warn(f"a profile without {KEY_COOKIE} is effectively new — expect a CAPTCHA on "
         f"its first request")
    print(f"     fix: python v3_distributed/warm_profiles.py --workers N --headed")


def captcha_report(store) -> None:
    print("\nBLOCKING (last 200 attempts)")
    print("-" * 62)
    rows = store.query("""
        SELECT status, count(*) n
        FROM (SELECT status FROM scrape_requests ORDER BY started_at DESC LIMIT 200) t
        GROUP BY status ORDER BY n DESC""")
    total = sum(r["n"] for r in rows) or 1
    for r in rows:
        line = f"  {r['status']:<16}{r['n']:>6}  {100*r['n']/total:>5.1f}%"
        (bad if r["status"] == "captcha" and r["n"] / total > 0.2 else print)(line) \
            if r["status"] == "captcha" else print(line)

    print("\n  by worker / mode")
    for r in store.query("""
        SELECT coalesce(worker_id,'(v2)') w, headless,
               count(*) n, count(*) FILTER (WHERE status='captcha') captcha
        FROM (SELECT * FROM scrape_requests ORDER BY started_at DESC LIMIT 200) t
        GROUP BY 1,2 ORDER BY 1"""):
        rate = 100 * r["captcha"] / max(1, r["n"])
        mode = "headless" if r["headless"] else "headed"
        flag = "  <-- high" if rate > 25 else ""
        print(f"    {r['w']:<8}{mode:<10}{r['n']:>5} attempts  {rate:>5.1f}% captcha{flag}")

    first = store.query("""
        SELECT count(*) n FROM scrape_requests q
        WHERE q.status='captcha' AND q.attempt = 1""")[0]["n"]
    if first:
        print(f"\n  {first} CAPTCHA(s) on a first attempt — that is the cold-profile "
              f"signature,\n  not rate limiting. Warm the profile before blaming volume.")


def saved_captchas(store, dump: bool) -> None:
    print("\nSAVED CAPTCHA PAGES")
    print("-" * 62)
    rows = store.query("""
        SELECT request_id, term, worker_id, started_at, html_path
        FROM scrape_requests
        WHERE status='captcha' AND html_path IS NOT NULL
        ORDER BY started_at DESC LIMIT 5""")
    if not rows:
        warn("none saved — CAPTCHA pages are only kept for attempts that returned a page")
        return
    for r in rows:
        print(f"  {str(r['request_id'])[:8]}  {r['started_at']:%H:%M:%S}  "
              f"{(r['worker_id'] or '-'):<5}  {r['term'][:28]:<30}{r['html_path']}")
    if dump:
        from db.store import read_html
        html = read_html(rows[0]["html_path"])
        print("\n--- latest CAPTCHA page (first 800 chars) ---")
        print(html[:800])


def main() -> int:
    ap = argparse.ArgumentParser(description="Diagnose CAPTCHAs and environment issues.")
    ap.add_argument("--captcha", action="store_true", help="Only the blocking analysis.")
    ap.add_argument("--open-captcha", action="store_true", help="Dump the latest CAPTCHA page.")
    args = ap.parse_args()

    print("=" * 62)
    print("SCRAPER DOCTOR")
    print("=" * 62)

    if not args.captcha:
        check_services()

    profile_report()

    try:
        from db.store import Store
        store = Store()
        captcha_report(store)
        saved_captchas(store, args.open_captcha)
        store.close()
    except Exception as exc:
        bad(f"could not read scrape history: {exc}")

    print("\nIF YOU ARE BEING BLOCKED")
    print("-" * 62)
    print("  1. warm the profile   python v3_distributed/warm_profiles.py --workers N --headed")
    print("  2. slow down          --min-delay 5 --max-delay 12")
    print("  3. run headed once    a visible window clears challenges headless cannot")
    print("  4. wait               blocking is per-IP and decays over ~15-30 min")
    print("  5. inspect the page   python doctor.py --open-captcha")
    return 0


if __name__ == "__main__":
    sys.exit(main())
