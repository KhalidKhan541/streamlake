"""Kafka producer that streams simulated events at a target rate."""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

from streamlake.config import Settings
from streamlake.events import generate_events

LOGGER = logging.getLogger(__name__)


def produce_stream(
    settings: Settings,
    rate: float = 20.0,
    duration: float = 30.0,
    seed: int | None = None,
    invalid_rate: float = 0.0,
) -> None:
    """Send ``rate * duration`` events to the configured topic."""
    if rate <= 0:
        raise ValueError("rate must be > 0")
    if duration <= 0:
        raise ValueError("duration must be > 0")

    total = max(1, int(rate * duration))
    effective_seed = seed if seed is not None else random.SystemRandom().randrange(1, 1_000_000)
    events = generate_events(
        total,
        seed=effective_seed,
        reference_time=datetime.now(UTC),
        lookback=timedelta(minutes=2),
        invalid_rate=invalid_rate,
    )

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "streamlake-producer",
            "acks": "all",
            "linger.ms": 50,
        }
    )
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, msg) -> None:
        if err is not None:
            stats["failed"] += 1
            LOGGER.error("delivery failed: %s", err)
        else:
            stats["delivered"] += 1

    started = time.monotonic()
    bucket = max(1, int(rate))
    for index, event in enumerate(events, start=1):
        producer.produce(
            settings.topic,
            value=json.dumps(event).encode("utf-8"),
            callback=on_delivery,
        )
        producer.poll(0)
        if index % bucket == 0:
            time.sleep(1.0)  # crude pacing to ~rate events per second

    pending = producer.flush(30)
    elapsed = time.monotonic() - started
    LOGGER.info(
        "produced %d event(s) in %.1fs | delivered=%d failed=%d pending=%d",
        len(events),
        elapsed,
        stats["delivered"],
        stats["failed"],
        pending,
    )
