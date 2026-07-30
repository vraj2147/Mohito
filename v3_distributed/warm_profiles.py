"""Pre-warm each worker's Chrome profile.

Google's block decision leans on cookie history, so a brand-new profile is
CAPTCHA'd almost immediately: in a two-worker run, w1 (warm) scraped 13 pages while
w2 (fresh) failed on its first request and never recovered. Adding workers
therefore adds cold profiles, and parallelism gets worse before it gets better.

This walks each profile through a few ordinary searches, accepting the CAPTCHAs
that come, until it strings together enough clean requests to be usable. Run it
once per worker id before a parallel batch.

    python v3_distributed/warm_profiles.py --workers 2
    python v3_distributed/warm_profiles.py --workers 4 --target 5 --headed
"""

from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import random
import sys
import time
from pathlib import Path

from google_session import GoogleSession

WARMUP_TERMS = [
    "weather", "bbc news", "train times", "recipes", "football results",
    "dictionary", "maps", "currency converter", "calculator", "time zones",
]


def warm(worker_id: str, target: int, headed: bool, max_tries: int,
         min_delay: float, max_delay: float) -> bool:
    profile = Path.home() / ".cache" / f"google_scraper_mq_{worker_id}"
    fresh = not profile.exists()
    print(f"[{worker_id}] profile {'NEW' if fresh else 'exists'}: {profile}", flush=True)

    clean = tries = 0
    with GoogleSession(headless=not headed, profile_dir=profile, scroll=False) as s:
        while clean < target and tries < max_tries:
            term = random.choice(WARMUP_TERMS)
            tries += 1
            try:
                s.search(term, timeout_ms=30000)
                clean += 1
                print(f"[{worker_id}] ok {clean}/{target} ({term})", flush=True)
            except Exception as exc:
                clean = 0
                name = type(exc).__name__
                print(f"[{worker_id}] {name} on {term!r} — streak reset", flush=True)
                time.sleep(20)
            time.sleep(random.uniform(min_delay, max_delay))

    ok = clean >= target
    print(f"[{worker_id}] {'WARM' if ok else 'NOT WARM'} after {tries} request(s)", flush=True)
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description="Warm worker Chrome profiles before a parallel run.")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--worker-ids", nargs="+", default=None,
                    help="Explicit ids instead of w1..wN.")
    ap.add_argument("--target", type=int, default=3, help="Consecutive clean requests required.")
    ap.add_argument("--max-tries", type=int, default=15)
    ap.add_argument("--min-delay", type=float, default=3.0)
    ap.add_argument("--max-delay", type=float, default=7.0)
    ap.add_argument("--headed", action="store_true",
                    help="Warm with a visible window — far more reliable for a cold profile.")
    args = ap.parse_args()

    ids = args.worker_ids or [f"w{i}" for i in range(1, args.workers + 1)]
    results = {}
    for wid in ids:
        results[wid] = warm(wid, args.target, args.headed, args.max_tries,
                            args.min_delay, args.max_delay)

    print("\nSUMMARY")
    for wid, ok in results.items():
        print(f"  {wid:<6}{'warm' if ok else 'COLD — expect CAPTCHAs'}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
