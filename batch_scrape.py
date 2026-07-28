"""Batch Google SERP scraper.

Runs a workload of N searches — the same term repeated many times and/or a spread of
different terms — and saves the raw rendered HTML of every SERP.

The raw HTML is the product. It is written to `<out-dir>/html/` before anything
parses it, so the scrape is never lost to a parser bug or a Google markup change.
Ad extraction (text ads + PLAs, via ad_extractor.py) runs as a convenience pass over
each page and its failures are recorded, not fatal. You can always re-derive any
metric offline from the saved HTML.

The whole job is one command:

    # 500 repeats of one term + 500 spread over the terms in queries.txt
    python batch_scrape.py --repeat-query "iPhone 16 Pro" --repeat 500 \\
        --query-file queries.txt --total 1000

    # resume an interrupted run (skips whatever is already in results.jsonl)
    python batch_scrape.py ... --resume

    # re-report from an existing run without re-scraping
    python batch_scrape.py --report-only --out-dir runs/myrun

Results stream to `results.jsonl` after every request, so an interrupted or blocked
run never loses completed work.

On blocking: Google rate-limits aggressive scraping. The runner uses randomised
delays, and on CAPTCHA it backs off exponentially and re-warms the session. Expect
some requests to fail at high volume; they are recorded as `captcha`/`error` rows
rather than aborting the run, and `--resume` will retry them on a later pass.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from ad_extractor import extract_ads
from google_search_html import CaptchaError, DEFAULT_PROFILE_DIR, slugify
from google_session import GoogleSession

_STOP = False


def _handle_sigint(signum, frame):
    """First Ctrl-C finishes the current request and shuts down cleanly."""
    global _STOP
    if _STOP:
        raise KeyboardInterrupt
    _STOP = True
    print("\nStopping after this request… (Ctrl-C again to force)", file=sys.stderr)


def build_workload(args) -> list[str]:
    """Assemble the ordered list of queries to run."""
    varied: list[str] = []
    if args.query_file:
        varied = [
            line.strip()
            for line in Path(args.query_file).read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        ]

    jobs: list[str] = []
    if args.repeat_query:
        jobs += [args.repeat_query] * args.repeat

    if varied:
        if args.total:
            # Fill the remaining budget by cycling the varied terms evenly.
            remaining = max(0, args.total - len(jobs))
            jobs += [varied[i % len(varied)] for i in range(remaining)]
        else:
            jobs += varied * args.varied_repeat

    if args.total:
        jobs = jobs[: args.total]
    if args.shuffle:
        random.shuffle(jobs)
    return jobs


def load_done(results_path: Path) -> tuple[list[dict], Counter]:
    """Read prior results for --resume. Returns (rows, per-query success counts)."""
    rows: list[dict] = []
    done = Counter()
    if not results_path.exists():
        return rows, done
    for line in results_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        rows.append(row)
        if row.get("status") == "ok":
            done[row.get("query", "")] += 1
    return rows, done


def run_batch(args) -> list[dict]:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results_path = out_dir / "results.jsonl"
    html_dir = out_dir / "html"
    if args.save_html:
        html_dir.mkdir(exist_ok=True)

    jobs = build_workload(args)
    if not jobs:
        print("Nothing to do — pass --repeat-query and/or --query-file.", file=sys.stderr)
        return []

    prior_rows, done = load_done(results_path) if args.resume else ([], Counter())
    if args.resume and done:
        # Drop as many instances of each query as already succeeded.
        remaining, budget = [], Counter(done)
        for q in jobs:
            if budget[q] > 0:
                budget[q] -= 1
            else:
                remaining.append(q)
        print(f"Resuming: {sum(done.values())} already done, {len(remaining)} left.")
        jobs = remaining

    print(f"Workload: {len(jobs)} requests over {len(set(jobs))} distinct queries.")
    print(f"Output:   {results_path}")

    signal.signal(signal.SIGINT, _handle_sigint)

    rows: list[dict] = list(prior_rows)
    stats = Counter()
    consecutive_captchas = 0
    started = time.time()

    fh = results_path.open("a", encoding="utf-8")
    session: GoogleSession | None = None
    try:
        session = GoogleSession(
            headless=not args.headed,
            reject_cookies=not args.accept_cookies,
            profile_dir=Path(args.profile_dir),
            browser_validation=args.browser_validation,
        ).__enter__()

        for i, query in enumerate(jobs, 1):
            if _STOP:
                break

            row = {
                "i": i,
                "query": query,
                "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            try:
                html = session.search(query)

                # The raw HTML is the product. Persist it before anything else touches
                # it, so a parser bug or a markup change can never cost a scrape —
                # extraction can always be re-run offline against these files.
                if args.save_html:
                    p = html_dir / f"{i:05d}_{slugify(query)}.html"
                    p.write_text(html, encoding="utf-8")
                    row["html_file"] = str(p.relative_to(out_dir))
                    row["html_bytes"] = len(html)

                row.update(status="ok")
                stats["ok"] += 1
                consecutive_captchas = 0

                # Ad extraction is a convenience layer over the saved HTML. If it
                # fails, the scrape still counts as a success.
                try:
                    ads = extract_ads(html, query=query)
                    row.update(
                        text_ad_count=ads.text_ad_count,
                        pla_count=ads.pla_count,
                        total_ads=ads.total_ads,
                        top_text_ads=ads.top_text_ads,
                        organic_count=ads.organic_count,
                        text_advertisers=[a.advertiser for a in ads.text_ads if a.advertiser],
                        pla_merchants=[p.merchant for p in ads.product_ads if p.merchant],
                        text_ads=[{"title": a.title, "advertiser": a.advertiser,
                                   "destination_url": a.destination_url,
                                   "placement": a.placement, "slot": a.slot}
                                  for a in ads.text_ads],
                        product_ads=[{"title": p.title, "price": p.price, "merchant": p.merchant,
                                      "destination_url": p.destination_url, "slot": p.slot}
                                     for p in ads.product_ads],
                    )
                except Exception as exc:
                    row["extract_error"] = f"{type(exc).__name__}: {exc}"[:200]
                    stats["extract_error"] += 1

            except CaptchaError:
                row.update(status="captcha")
                stats["captcha"] += 1
                consecutive_captchas += 1

            except Exception as exc:  # keep the run alive on transient page errors
                row.update(status="error", error=f"{type(exc).__name__}: {exc}"[:300])
                stats["error"] += 1

            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            fh.flush()
            rows.append(row)

            if i % args.progress_every == 0 or i == len(jobs):
                rate = stats["ok"] / max(1, i)
                elapsed = time.time() - started
                print(
                    f"[{i}/{len(jobs)}] ok={stats['ok']} captcha={stats['captcha']} "
                    f"err={stats['error']} | success {rate:.0%} | "
                    f"{elapsed / max(1, i):.1f}s/req"
                )

            # Escalate headless -> headed before falling back to pure backoff. A
            # visible window clears interstitials that waiting alone will not.
            if (
                consecutive_captchas >= args.escalate_after
                and session.headless
                and not args.no_escalate
            ):
                print(
                    f"  {consecutive_captchas} CAPTCHAs in headless — relaunching with a "
                    f"visible window for the rest of the run.",
                    file=sys.stderr,
                )
                session.restart(headless=False)
                consecutive_captchas = 0
                time.sleep(args.backoff)
                continue

            if consecutive_captchas >= args.captcha_limit:
                print(
                    f"Hit {consecutive_captchas} CAPTCHAs in a row — Google is blocking this "
                    f"session. Stopping; re-run with --resume later.",
                    file=sys.stderr,
                )
                break

            if consecutive_captchas:
                # Exponential backoff, capped, then rebuild the session so the next
                # attempt starts from a fresh page state.
                backoff = min(args.max_backoff, args.backoff * (2 ** (consecutive_captchas - 1)))
                print(f"  CAPTCHA — backing off {backoff:.0f}s", file=sys.stderr)
                time.sleep(backoff)
            elif i < len(jobs):
                time.sleep(random.uniform(args.min_delay, args.max_delay))

    finally:
        if session is not None:
            session.close()
        fh.close()

    return rows


def summarise(rows: list[dict], out_dir: Path) -> None:
    """Print and save the ad-rate report."""
    ok = [r for r in rows if r.get("status") == "ok"]
    n_captcha = sum(1 for r in rows if r.get("status") == "captcha")
    n_error = sum(1 for r in rows if r.get("status") == "error")

    if not ok:
        print("\nNo successful SERPs to report on.", file=sys.stderr)
        return

    n = len(ok)
    with_text = sum(1 for r in ok if r["text_ad_count"] > 0)
    with_pla = sum(1 for r in ok if r["pla_count"] > 0)
    with_any = sum(1 for r in ok if r["total_ads"] > 0)
    with_both = sum(1 for r in ok if r["text_ad_count"] > 0 and r["pla_count"] > 0)
    tot_text = sum(r["text_ad_count"] for r in ok)
    tot_pla = sum(r["pla_count"] for r in ok)
    tot_top = sum(r.get("top_text_ads", 0) for r in ok)

    print("\n" + "=" * 72)
    print("SPONSORED AD RATE REPORT")
    print("=" * 72)
    print(f"SERPs scraped OK      : {n}")
    print(f"CAPTCHA / error       : {n_captcha} / {n_error}")
    print()
    print(f"TEXT AD RATE          : {with_text / n:>7.1%}  ({with_text}/{n} SERPs carried >=1 text ad)")
    print(f"PLA RATE              : {with_pla / n:>7.1%}  ({with_pla}/{n} SERPs carried >=1 shopping ad)")
    print(f"ANY AD RATE           : {with_any / n:>7.1%}  ({with_any}/{n})")
    print(f"BOTH FORMATS          : {with_both / n:>7.1%}  ({with_both}/{n})")
    print()
    print(f"Avg text ads / SERP   : {tot_text / n:>7.2f}   (total {tot_text})")
    print(f"Avg PLAs / SERP       : {tot_pla / n:>7.2f}   (total {tot_pla})")
    if tot_text:
        print(f"Text ads in TOP slot  : {tot_top / tot_text:>7.1%}  ({tot_top}/{tot_text})")

    # Per-query breakdown
    per = defaultdict(list)
    for r in ok:
        per[r["query"]].append(r)

    print("\n" + "-" * 72)
    print(f"{'QUERY':<34}{'N':>5}{'TXT%':>7}{'PLA%':>7}{'txt/s':>7}{'pla/s':>7}")
    print("-" * 72)
    for q, rs in sorted(per.items(), key=lambda kv: -len(kv[1])):
        m = len(rs)
        t_rate = sum(1 for r in rs if r["text_ad_count"] > 0) / m
        p_rate = sum(1 for r in rs if r["pla_count"] > 0) / m
        print(
            f"{q[:33]:<34}{m:>5}{t_rate:>6.0%}{p_rate:>7.0%}"
            f"{sum(r['text_ad_count'] for r in rs) / m:>7.2f}"
            f"{sum(r['pla_count'] for r in rs) / m:>7.2f}"
        )

    # Who is advertising
    advertisers = Counter(a for r in ok for a in r.get("text_advertisers", []))
    merchants = Counter(m for r in ok for m in r.get("pla_merchants", []))
    for label, counter in (("TOP TEXT-AD ADVERTISERS", advertisers), ("TOP PLA MERCHANTS", merchants)):
        if not counter:
            continue
        print(f"\n{label}")
        print("-" * 72)
        total = sum(counter.values())
        for name, c in counter.most_common(10):
            print(f"  {name[:44]:<46}{c:>6}  {c / total:>6.1%}")

    summary = {
        "serps_ok": n,
        "captcha": n_captcha,
        "errors": n_error,
        "text_ad_rate": with_text / n,
        "pla_rate": with_pla / n,
        "any_ad_rate": with_any / n,
        "both_formats_rate": with_both / n,
        "avg_text_ads_per_serp": tot_text / n,
        "avg_plas_per_serp": tot_pla / n,
        "total_text_ads": tot_text,
        "total_plas": tot_pla,
        "per_query": {
            q: {
                "serps": len(rs),
                "text_ad_rate": sum(1 for r in rs if r["text_ad_count"] > 0) / len(rs),
                "pla_rate": sum(1 for r in rs if r["pla_count"] > 0) / len(rs),
                "avg_text_ads": sum(r["text_ad_count"] for r in rs) / len(rs),
                "avg_plas": sum(r["pla_count"] for r in rs) / len(rs),
            }
            for q, rs in per.items()
        },
        "top_text_advertisers": advertisers.most_common(25),
        "top_pla_merchants": merchants.most_common(25),
    }
    path = out_dir / "ad_rate_summary.json"
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    csv_path = out_dir / "per_serp.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("i,query,status,text_ad_count,pla_count,total_ads,top_text_ads,organic_count\n")
        for r in rows:
            q = '"' + str(r.get("query", "")).replace('"', '""') + '"'
            f.write(
                f"{r.get('i','')},{q},{r.get('status','')},{r.get('text_ad_count','')},"
                f"{r.get('pla_count','')},{r.get('total_ads','')},"
                f"{r.get('top_text_ads','')},{r.get('organic_count','')}\n"
            )
    print(f"\nWrote {path}\nWrote {csv_path}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Batch-scrape Google SERPs and report text-ad / PLA rates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    w = ap.add_argument_group("workload")
    w.add_argument("--repeat-query", default=None, help="A single term to search repeatedly.")
    w.add_argument("--repeat", type=int, default=0, help="How many times to repeat --repeat-query.")
    w.add_argument("--query-file", type=Path, default=None, help="File of distinct queries, one per line.")
    w.add_argument("--varied-repeat", type=int, default=1, help="Passes over --query-file (ignored if --total set).")
    w.add_argument("--total", type=int, default=None, help="Hard cap on total requests.")
    w.add_argument("--shuffle", action="store_true", help="Interleave queries instead of running them in blocks.")

    p = ap.add_argument_group("pacing")
    p.add_argument("--min-delay", type=float, default=3.0, help="Min seconds between requests (default 3).")
    p.add_argument("--max-delay", type=float, default=8.0, help="Max seconds between requests (default 8).")
    p.add_argument("--backoff", type=float, default=60.0, help="Base CAPTCHA backoff seconds (default 60).")
    p.add_argument("--max-backoff", type=float, default=900.0, help="Backoff ceiling (default 900).")
    p.add_argument("--captcha-limit", type=int, default=5, help="Consecutive CAPTCHAs before giving up (default 5).")
    p.add_argument("--escalate-after", type=int, default=2,
                   help="Consecutive CAPTCHAs before switching headless->headed (default 2).")
    p.add_argument("--no-escalate", action="store_true",
                   help="Never auto-switch to a headed window; back off only.")

    o = ap.add_argument_group("output")
    o.add_argument("--out-dir", type=Path, default=Path("runs/latest"), help="Output directory.")
    o.add_argument("--no-save-html", dest="save_html", action="store_false",
                   help="Do NOT keep the raw SERP HTML. Off by default: the HTML is the "
                        "primary artefact, so it is saved unless you opt out.")
    o.set_defaults(save_html=True)
    o.add_argument("--resume", action="store_true", help="Skip queries already completed in this out-dir.")
    o.add_argument("--report-only", action="store_true", help="Re-report from existing results.jsonl; no scraping.")
    o.add_argument("--progress-every", type=int, default=10, help="Progress line frequency.")

    b = ap.add_argument_group("browser")
    b.add_argument("--headed", action="store_true", help="Visible window (far less likely to be CAPTCHA'd).")
    b.add_argument("--accept-cookies", action="store_true", help="Accept cookies instead of rejecting.")
    b.add_argument("--profile-dir", default=str(DEFAULT_PROFILE_DIR), help="Persistent Chrome profile.")
    b.add_argument("--browser-validation", default=None, help="Captured x-browser-validation header value.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)

    if args.report_only:
        rows, _ = load_done(out_dir / "results.jsonl")
        if not rows:
            print(f"No results.jsonl in {out_dir}", file=sys.stderr)
            return 1
    else:
        rows = run_batch(args)
        if not rows:
            return 1

    summarise(rows, out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
