"""RabbitMQ topology and connection helpers.

Topology:

    scrape.jobs      main work queue, consumed by scraper workers
    scrape.retry     holds failed jobs for retry_ttl_ms, then dead-letters
                     back into scrape.jobs — a delayed retry
    scrape.dead      jobs that exhausted their attempts; nothing consumes it
    extract.jobs     scrape outcomes awaiting extraction
    extract.dead     extraction failures

Retry is done with a TTL queue rather than nack(requeue=True). An immediately
requeued job is redelivered instantly, which against a rate-limiting target turns
one CAPTCHA into a hot loop. Parking the message in scrape.retry with a TTL gives
a real backoff with no consumer spinning.

Everything is declared durable and published persistent, so a broker restart does
not drop queued work.
"""


from __future__ import annotations

import sys as _sys, pathlib as _pl
_ROOT = _pl.Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "lib"), str(_ROOT / "v3_distributed")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

import json
import os
import uuid

import pika

DEFAULT_URL = os.environ.get("SCRAPER_AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")

JOBS_QUEUE = "scrape.jobs"
RETRY_QUEUE = "scrape.retry"
DEAD_QUEUE = "scrape.dead"
EXTRACT_QUEUE = "extract.jobs"
EXTRACT_DEAD_QUEUE = "extract.dead"

DEFAULT_RETRY_TTL_MS = 60_000


def connect(url: str = DEFAULT_URL, heartbeat: int = 600) -> pika.BlockingConnection:
    """Open a connection.

    A long heartbeat and blocked-connection timeout matter here: a scrape can hold
    the channel for tens of seconds, and the default 60s heartbeat will drop a
    connection that is merely busy rather than dead.
    """
    params = pika.URLParameters(url)
    params.heartbeat = heartbeat
    params.blocked_connection_timeout = 300
    return pika.BlockingConnection(params)


def declare_topology(channel, retry_ttl_ms: int = DEFAULT_RETRY_TTL_MS) -> None:
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)
    channel.queue_declare(queue=EXTRACT_DEAD_QUEUE, durable=True)

    channel.queue_declare(
        queue=JOBS_QUEUE,
        durable=True,
        arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": DEAD_QUEUE},
    )
    channel.queue_declare(
        queue=RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": retry_ttl_ms,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": JOBS_QUEUE,
        },
    )
    channel.queue_declare(
        queue=EXTRACT_QUEUE,
        durable=True,
        arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": EXTRACT_DEAD_QUEUE},
    )


def publish(channel, queue: str, payload: dict, message_id: str | None = None) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=json.dumps(payload, default=str).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=message_id or str(uuid.uuid4()),
        ),
    )


def queue_depth(channel, queue: str) -> int:
    return channel.queue_declare(queue=queue, durable=True, passive=True).method.message_count


def depths(channel) -> dict[str, int]:
    out = {}
    for q in (JOBS_QUEUE, RETRY_QUEUE, DEAD_QUEUE, EXTRACT_QUEUE, EXTRACT_DEAD_QUEUE):
        try:
            out[q] = queue_depth(channel, q)
        except Exception:
            out[q] = -1
    return out


def purge_all(channel) -> None:
    for q in (JOBS_QUEUE, RETRY_QUEUE, DEAD_QUEUE, EXTRACT_QUEUE, EXTRACT_DEAD_QUEUE):
        try:
            channel.queue_purge(q)
        except Exception:
            pass
