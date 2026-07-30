"""Job ledger.

RabbitMQ dispatches work; this table records it. Every job's whole lifecycle is a
row, so at any instant three questions have exact answers: what finished, what is
in flight and on which worker, and what is left.

The ledger is deliberately written before and after the scrape, never only after.
A worker killed mid-request leaves an `in_flight` row whose lease expires, which
`reclaim_expired` turns back into work. Without that, a dropped job exists only as
an unacked AMQP message and is invisible.

State machine:

    pending ──publish──> queued ──claim──> in_flight ──scraped──> scraped
                            ^                  │                     │
                            │                  ├──retryable fail──> retry ──┐
                            └──────────────────┴──lease expiry──────────────┘
                                               │                     │
                                               └──attempts exhausted─┴──> dead
                                                                     │
                                                       extract ──────┴──> extracted
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from db.store import Store

TERMINAL = ("extracted", "dead")
RESUMABLE = ("pending", "queued", "retry")


class JobStore(Store):
    def create_batch(self, config: dict, notes: str | None = None) -> tuple[int, uuid.UUID]:
        import json

        batch_uuid = uuid.uuid4()
        row = self.query(
            "INSERT INTO scrape_batches (batch_uuid, config, notes) VALUES (%s, %s, %s) "
            "RETURNING id",
            (batch_uuid, json.dumps(config, default=str), notes),
        )[0]
        return row["id"], batch_uuid

    def add_jobs(self, batch_id: int, jobs: list[dict], max_attempts: int = 3) -> list[dict]:
        """Insert job rows as `pending`. Rows exist before anything is published, so
        a crash between insert and publish leaves recoverable work rather than a
        silent gap."""
        rows = [
            {
                "job_id": uuid.uuid4(),
                "batch_id": batch_id,
                "term_id": j.get("term_id"),
                "term": j["term"],
                "locale": j.get("locale", "en-GB"),
                "max_attempts": max_attempts,
            }
            for j in jobs
        ]
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO scrape_jobs (job_id, batch_id, term_id, term, locale, max_attempts) "
                "VALUES (%(job_id)s, %(batch_id)s, %(term_id)s, %(term)s, %(locale)s, %(max_attempts)s)",
                rows,
            )
        return rows

    def mark_published(self, job_ids: list[uuid.UUID]) -> int:
        return self.execute(
            "UPDATE scrape_jobs SET status='queued', published_at=now(), updated_at=now() "
            "WHERE job_id = ANY(%s) AND status IN ('pending','retry')",
            (job_ids,),
        )

    def unpublished(self, batch_id: int | None = None) -> list[dict]:
        """Jobs whose row exists but which never reached the broker, plus anything
        parked for retry. This is what the loader republishes on a resume."""
        sql = ("SELECT job_id, batch_id, term_id, term, locale, attempts FROM scrape_jobs "
               "WHERE status IN ('pending','retry')")
        params: tuple = ()
        if batch_id is not None:
            sql += " AND batch_id = %s"
            params = (batch_id,)
        return self.query(sql + " ORDER BY created_at", params)

    def claim(self, job_id: uuid.UUID, worker_id: str, lease_seconds: int = 300) -> dict | None:
        """Take ownership of a job. Returns None when another worker already holds
        it or it is already finished, which is how a redelivered message is
        rejected instead of scraped twice."""
        expires = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        rows = self.query(
            """UPDATE scrape_jobs
               SET status='in_flight', worker_id=%s, lease_expires_at=%s,
                   attempts=attempts+1, updated_at=now()
               WHERE job_id=%s AND status IN ('pending','queued','retry','in_flight')
               RETURNING job_id, term, term_id, locale, attempts, max_attempts""",
            (worker_id, expires, job_id),
        )
        return rows[0] if rows else None

    def renew_lease(self, job_id: uuid.UUID, lease_seconds: int = 300) -> None:
        expires = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        self.execute(
            "UPDATE scrape_jobs SET lease_expires_at=%s, updated_at=now() WHERE job_id=%s",
            (expires, job_id),
        )

    def mark_scraped(self, job_id: uuid.UUID, request_id: uuid.UUID,
                     html_path: str | None, last_status: str) -> None:
        self.execute(
            """UPDATE scrape_jobs
               SET status='scraped', request_id=%s, html_path=%s, last_status=%s,
                   scraped_at=now(), updated_at=now(), lease_expires_at=NULL
               WHERE job_id=%s""",
            (request_id, html_path, last_status, job_id),
        )

    def mark_failed(self, job_id: uuid.UUID, request_id: uuid.UUID | None,
                    last_status: str, error: str | None) -> str:
        """Park a failed attempt. Returns the resulting status: 'retry' while
        attempts remain, otherwise 'dead'."""
        rows = self.query(
            """UPDATE scrape_jobs
               SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'retry' END,
                   request_id=%s, last_status=%s, last_error=left(%s, 2000),
                   updated_at=now(), lease_expires_at=NULL, worker_id=NULL
               WHERE job_id=%s
               RETURNING status""",
            (request_id, last_status, error, job_id),
        )
        return rows[0]["status"] if rows else "dead"

    def mark_extracted(self, job_id: uuid.UUID) -> None:
        self.execute(
            "UPDATE scrape_jobs SET status='extracted', extracted_at=now(), updated_at=now() "
            "WHERE job_id=%s",
            (job_id,),
        )

    def reclaim_expired(self, grace_seconds: int = 0) -> list[dict]:
        """Return jobs whose worker died holding the lease to the retry pool.

        This is the recovery path for a killed worker, an OOM, or a machine that
        slept mid-run — cases where no failure message is ever published.
        """
        return self.query(
            """UPDATE scrape_jobs
               SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'retry' END,
                   worker_id=NULL, lease_expires_at=NULL,
                   last_status='lease_expired', updated_at=now()
               WHERE status='in_flight'
                 AND lease_expires_at < now() - make_interval(secs => %s)
               RETURNING job_id, batch_id, term_id, term, locale, attempts, status""",
            (grace_seconds,),
        )

    def progress(self, batch_id: int | None = None) -> list[dict]:
        sql = "SELECT * FROM v_batch_progress"
        params: tuple = ()
        if batch_id is not None:
            sql += " WHERE batch_id = %s"
            params = (batch_id,)
        return self.query(sql + " ORDER BY batch_id", params)

    def latest_batch(self) -> int | None:
        rows = self.query("SELECT id FROM scrape_batches ORDER BY id DESC LIMIT 1")
        return rows[0]["id"] if rows else None
