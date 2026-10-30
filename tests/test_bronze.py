"""Bronze and dead-letter Parquet writer tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd

from streamlake.bronze import BronzeWriter, DeadLetter, DeadLetterWriter
from streamlake.config import Settings
from streamlake.events import EventBase, validate_event


def _page_view(event_id: str, event_ts: str) -> EventBase:
    return validate_event(
        {
            "event_id": event_id,
            "event_type": "page_view",
            "event_ts": event_ts,
            "user_id": "u_1",
            "session_id": "s_1",
            "page": "/",
            "device": "desktop",
        }
    )


def test_bronze_is_partitioned_by_event_date(settings: Settings) -> None:
    events = [
        _page_view("evt_1", "2026-10-28T23:59:00+00:00"),
        _page_view("evt_2", "2026-10-29T00:01:00+00:00"),
        _page_view("evt_3", "2026-10-29T09:00:00+00:00"),
    ]

    paths = BronzeWriter(settings.bronze_dir).write(events, batch_id="test-batch")

    assert len(paths) == 2
    assert paths[0].parent.name == "event_date=2026-10-28"
    assert paths[1].parent.name == "event_date=2026-10-29"

    frame = pd.read_parquet(paths[1])
    assert list(frame.columns) == [
        "event_id",
        "event_type",
        "event_ts",
        "user_id",
        "session_id",
        "payload",
        "ingested_at",
        "batch_id",
    ]
    assert sorted(frame["event_id"]) == ["evt_2", "evt_3"]
    assert set(frame["batch_id"]) == {"test-batch"}

    payload = json.loads(frame.loc[frame["event_id"] == "evt_2", "payload"].iloc[0])
    assert payload == {"page": "/", "device": "desktop"}


def test_bronze_writer_returns_nothing_for_an_empty_batch(settings: Settings) -> None:
    assert BronzeWriter(settings.bronze_dir).write([]) == []


def test_dlq_keeps_the_raw_payload_and_error(settings: Settings) -> None:
    received_at = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
    records = [
        DeadLetter(
            raw='{"event_id": "evt_1", "event_type": "page_view"}',
            error="session_id: Field required",
            received_at=received_at,
        )
    ]

    paths = DeadLetterWriter(settings.dlq_dir).write(records, batch_id="dlq-batch")

    assert len(paths) == 1
    assert paths[0].parent.name == "received_date=2026-10-29"

    frame = pd.read_parquet(paths[0])
    assert list(frame.columns) == ["raw", "error", "received_at", "batch_id"]
    assert frame.loc[0, "raw"] == records[0].raw
    assert frame.loc[0, "error"] == records[0].error
    assert frame.loc[0, "batch_id"] == "dlq-batch"
    assert frame.loc[0, "received_at"] == pd.Timestamp(received_at)
