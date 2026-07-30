"""Data-access layer for the scraper.

Everything the scraper needs from Postgres lives here, so swapping engines or
adding a second backend means touching one file.

Two invariants this module enforces:

* Raw HTML is written to disk *before* its row is inserted, and the file is named
  after the request_id. If the process dies between the two, the page is still on
  disk and recoverable — losing a scrape is expensive, losing a row is not.

* Every attempt is recorded. `record_attempt` is called for failures exactly as it
  is for successes; the classified `status` is what error metrics are built on.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DEFAULT_DSN = os.environ.get("SCRAPER_DSN", "postgresql:///google_scraper")

# Maps an exception to the constrained `status` vocabulary in schema.sql. Order
# matters: the first substring match wins, so put specific patterns first.
_ERROR_PATTERNS = (
    ("captcha", "captcha"),
    ("ERR_NAME_NOT_RESOLVED", "dns_error"),
    ("ERR_INTERNET_DISCONNECTED", "network_error"),
    ("ERR_NETWORK_CHANGED", "network_error"),
    ("ERR_CONNECTION", "network_error"),
    ("ERR_PROXY", "network_error"),
    ("Timeout", "nav_timeout"),
    ("Target closed", "browser_crash"),
    ("Browser closed", "browser_crash"),
    ("crashed", "browser_crash"),
)


def classify_error(exc: BaseException) -> tuple[str, str, str]:
    """Return (status, error_class, error_message) for an exception.

    Free-text error strings do not aggregate into metrics, so every failure is
    folded into one of the statuses the schema allows.
    """
    cls = type(exc).__name__
    msg = str(exc)
    haystack = f"{cls} {msg}"
    if cls == "CaptchaError":
        return "captcha", cls, msg[:2000]
    for needle, status in _ERROR_PATTERNS:
        if needle.lower() in haystack.lower():
            return status, cls, msg[:2000]
    return "unknown", cls, msg[:2000]


def git_sha() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


@dataclass
class HtmlArtifact:
    path: Path
    byte_len: int  # length of the *uncompressed* HTML
    sha256: str


def write_html(html: str, root: Path, request_id: uuid.UUID, term_slug: str,
               compress: bool = True) -> HtmlArtifact:
    """Persist one page under `root`, named by request_id.

    Naming by request_id (not by index) keeps the disk artefact and the DB row
    joined by a single key. The slug is appended purely so the directory is
    browsable by eye.
    """
    root.mkdir(parents=True, exist_ok=True)
    raw = html.encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()

    suffix = ".html.gz" if compress else ".html"
    path = root / f"{request_id}__{term_slug[:60]}{suffix}"
    if compress:
        # mtime=0 so identical HTML yields an identical file on disk.
        with gzip.GzipFile(filename="", mode="wb", fileobj=path.open("wb"), mtime=0) as fh:
            fh.write(raw)
    else:
        path.write_bytes(raw)

    return HtmlArtifact(path=path, byte_len=len(raw), sha256=digest)


def read_html(path: str | Path) -> str:
    """Read a page back, transparently handling gzip."""
    p = Path(path)
    if p.suffix == ".gz":
        with gzip.open(p, "rt", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    return p.read_text(encoding="utf-8", errors="replace")


class Store:
    def __init__(self, dsn: str = DEFAULT_DSN):
        self.dsn = dsn
        self.conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------- terms
    def upsert_terms(self, rows: list[dict]) -> int:
        """Insert or update search terms. `rows` need `term`; the rest are optional."""
        sql = """
        INSERT INTO search_terms (term, locale, target_repeats, priority, category, intent)
        VALUES (%(term)s, %(locale)s, %(target_repeats)s, %(priority)s, %(category)s, %(intent)s)
        ON CONFLICT (term, locale) DO UPDATE SET
            target_repeats = EXCLUDED.target_repeats,
            priority       = EXCLUDED.priority,
            category       = EXCLUDED.category,
            intent         = EXCLUDED.intent,
            active         = true
        """
        payload = [
            {
                "term": r["term"],
                "locale": r.get("locale", "en-GB"),
                "target_repeats": r.get("target_repeats", 1),
                "priority": r.get("priority", 0),
                "category": r.get("category"),
                "intent": r.get("intent"),
            }
            for r in rows
        ]
        with self.conn.cursor() as cur:
            cur.executemany(sql, payload)
        return len(payload)

    def pending_work(self, limit: int | None = None) -> list[dict]:
        """Terms still short of their target_repeats, highest priority first."""
        sql = "SELECT * FROM v_pending_work ORDER BY priority DESC, term_id"
        params: tuple = ()
        if limit:
            sql += " LIMIT %s"
            params = (limit,)
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def build_queue(
        self,
        shuffle: bool = True,
        cap: int | None = None,
        term: str | None = None,
        distinct: bool = False,
    ) -> list[dict]:
        """Expand pending work into a flat list of individual jobs.

        A term needing 500 more scrapes contributes 500 entries. Shuffling matters
        for block avoidance: 500 identical consecutive queries is a far stronger
        bot signal than the same 500 scattered among others.

        `term` restricts the queue to a single search term; `distinct` caps every
        term at one job. The two together let you run a repeat-heavy batch and a
        breadth batch separately and compare them.
        """
        import random

        jobs: list[dict] = []
        for row in self.pending_work():
            if term is not None and row["term"] != term:
                continue
            n = 1 if distinct else int(row["remaining"])
            jobs += [{"term_id": row["term_id"], "term": row["term"]}] * n
        if shuffle:
            random.shuffle(jobs)
        return jobs[:cap] if cap else jobs

    # -------------------------------------------------------------- runs
    def start_run(self, config: dict, notes: str | None = None) -> tuple[int, uuid.UUID]:
        run_uuid = uuid.uuid4()
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO scrape_runs (run_uuid, config, git_sha, notes)
                   VALUES (%s, %s, %s, %s) RETURNING id""",
                (run_uuid, json.dumps(config, default=str), git_sha(), notes),
            )
            return cur.fetchone()["id"], run_uuid

    def finish_run(self, run_id: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE scrape_runs SET finished_at = now() WHERE id = %s", (run_id,))

    # ---------------------------------------------------------- attempts
    def record_attempt(
        self,
        request_id: uuid.UUID,
        run_id: int,
        term: str,
        status: str,
        term_id: int | None = None,
        attempt: int = 1,
        error_class: str | None = None,
        error_message: str | None = None,
        final_url: str | None = None,
        started_at: datetime | None = None,
        duration_ms: int | None = None,
        html: HtmlArtifact | None = None,
        headless: bool | None = None,
        proxy: str | None = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scrape_requests (
                    request_id, run_id, term_id, term, attempt, status,
                    error_class, error_message, final_url,
                    started_at, finished_at, duration_ms,
                    html_path, html_bytes, html_sha256, headless, proxy
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, now(), %s,
                    %s, %s, %s, %s, %s
                )
                """,
                (
                    request_id, run_id, term_id, term, attempt, status,
                    error_class, error_message, final_url,
                    started_at or datetime.now(timezone.utc), duration_ms,
                    str(html.path) if html else None,
                    html.byte_len if html else None,
                    html.sha256 if html else None,
                    headless, proxy,
                ),
            )

    def record_serp(self, request_id: uuid.UUID, ads) -> None:
        """Store the parsed per-SERP counts and the individual ads."""
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO serp_results (request_id, sponsored_result_count, sponsored_product_count,
                                             total_ads, top_sponsored_results, organic_count)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (request_id) DO NOTHING""",
                (request_id, ads.sponsored_result_count, ads.sponsored_product_count, ads.total_ads,
                 ads.top_sponsored_results, ads.organic_count),
            )
            rows = [
                (request_id, "sponsored_result", a.placement, a.slot, a.title,
                 a.advertiser, None, a.destination_url)
                for a in ads.sponsored_results
            ] + [
                (request_id, "sponsored_product", p.placement, p.slot, p.title,
                 p.merchant, p.price, p.destination_url)
                for p in ads.sponsored_products
            ]
            if rows:
                cur.executemany(
                    """INSERT INTO serp_ads (request_id, ad_type, placement, slot,
                                             title, advertiser, price, destination_url)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    rows,
                )

    # --------------------------------------------------------- retrieval
    def get_html(self, request_id: uuid.UUID | str) -> str | None:
        """Raw HTML for a request, fetched via the path stored on its row.

        This is the "go from the result table straight to the raw page" path.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT html_path FROM scrape_requests WHERE request_id = %s",
                (str(request_id),),
            )
            row = cur.fetchone()
        if not row or not row["html_path"]:
            return None
        return read_html(row["html_path"])

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def execute(self, sql: str, params: tuple = ()) -> int:
        """Run a statement that returns no rows (DELETE/UPDATE); returns rowcount."""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount
