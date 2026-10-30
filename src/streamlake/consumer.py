"""Kafka consumer: validate -> write bronze/DLQ -> commit offsets.

Commit ordering is the core reliability guarantee: offsets are committed only
after the batch has been durably written. A crash therefore causes
redelivery, never data loss; silver deduplicates on ``event_id`` so
redelivery is harmless.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer
from pydantic import ValidationError

from streamlake.bronze import BronzeWriter, DeadLetter, DeadLetterWriter
from streamlake.config import Settings
from streamlake.events import EventBase, summarize_validation_error, validate_event
from streamlake.transform import transform

LOGGER = logging.getLogger(__name__)


def consume_stream(
    settings: Settings,
    max_messages: int | None = None,
    transform_every: int = 0,
    stop_after: float | None = None,
) -> None:
    """Consume events until interrupted or a test limit is reached."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    writer = BronzeWriter(settings.bronze_dir)
    dlq_writer = DeadLetterWriter(settings.dlq_dir)

    buffer: list[Any] = []
    processed = 0
    flushes = 0
    stop = False
    started = time.monotonic()
    last_flush = started

    def handle_stop(signum: int, frame: object) -> None:
        nonlocal stop
        LOGGER.info("received signal %s, shutting down after the current flush", signum)
        stop = True

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    def flush() -> None:
        nonlocal flushes, last_flush
        if not buffer:
            last_flush = time.monotonic()
            return

        ingested_at = datetime.now(UTC)
        valid, dead = _split_messages(buffer, ingested_at)
        if valid:
            writer.write(valid, ingested_at=ingested_at)
        if dead:
            dlq_writer.write(dead, ingested_at=ingested_at)

        # Durability reached - now it is safe to acknowledge the offsets.
        consumer.commit(asynchronous=False)
        buffer.clear()
        flushes += 1
        last_flush = time.monotonic()
        LOGGER.info(
            "flush #%d | valid=%d dead_letter=%d processed=%d",
            flushes,
            len(valid),
            len(dead),
            processed,
        )

        if transform_every > 0 and flushes % transform_every == 0:
            try:
                runs = transform(settings)
                LOGGER.info("transformed %d model(s)", len(runs))
            except Exception:
                LOGGER.exception(
                    "transform failed; bronze data is safe and the next cycle will retry"
                )

    consumer.subscribe([settings.topic])
    LOGGER.info(
        "consuming topic=%s bootstrap=%s group=%s",
        settings.topic,
        settings.kafka_bootstrap,
        settings.group_id,
    )

    try:
        while not stop:
            message = consumer.poll(0.5)
            now = time.monotonic()

            if message is not None:
                if message.error():
                    LOGGER.error("consumer error: %s", message.error())
                else:
                    buffer.append(message)
                    processed += 1

            due = buffer and (
                len(buffer) >= settings.batch_size
                or now - last_flush >= settings.flush_interval_seconds
            )
            if due:
                flush()

            if max_messages is not None and processed >= max_messages:
                break
            if stop_after is not None and now - started >= stop_after:
                break
    finally:
        try:
            flush()
        except Exception:
            LOGGER.exception("final flush failed")
        consumer.close()
        LOGGER.info("consumer stopped after %d message(s)", processed)


def _split_messages(
    messages: Sequence[Any], ingested_at: datetime
) -> tuple[list[EventBase], list[DeadLetter]]:
    valid: list[EventBase] = []
    dead: list[DeadLetter] = []

    for message in messages:
        value = message.value() or b""
        text = value.decode("utf-8", errors="replace")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as exc:
            dead.append(
                DeadLetter(
                    raw=text,
                    error=f"invalid json: {exc.msg}",
                    received_at=ingested_at,
                )
            )
            continue

        try:
            valid.append(validate_event(payload))
        except ValidationError as exc:
            dead.append(
                DeadLetter(
                    raw=text,
                    error=summarize_validation_error(exc),
                    received_at=ingested_at,
                )
            )

    return valid, dead
