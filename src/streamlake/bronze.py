"""Parquet writers for the immutable bronze layer and the dead-letter queue."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from streamlake.events import EventBase


@dataclass(frozen=True)
class DeadLetter:
    """An input record that failed validation, kept for inspection and replay."""

    raw: str
    error: str
    received_at: datetime


class BronzeWriter:
    """Writes validated events as one Parquet file per (batch, event date)."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def write(
        self,
        events: Sequence[EventBase],
        batch_id: str | None = None,
        ingested_at: datetime | None = None,
    ) -> list[Path]:
        if not events:
            return []

        ingested = ingested_at or datetime.now(UTC)
        batch = batch_id or f"{ingested:%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"

        by_date: dict[date, list[EventBase]] = defaultdict(list)
        for event in events:
            by_date[event.event_ts.date()].append(event)

        paths: list[Path] = []
        for event_date, day_events in sorted(by_date.items()):
            rows = [
                {
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "event_ts": event.event_ts,
                    "user_id": event.user_id,
                    "session_id": event.session_id,
                    "payload": event.payload_json(),
                    "ingested_at": ingested,
                    "batch_id": batch,
                }
                for event in day_events
            ]
            path = self.root / f"event_date={event_date.isoformat()}" / f"batch-{batch}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_parquet(path, index=False)
            paths.append(path)

        return paths


class DeadLetterWriter:
    """Writes invalid records to Parquet, partitioned by received date."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def write(
        self,
        records: Sequence[DeadLetter],
        batch_id: str | None = None,
        ingested_at: datetime | None = None,
    ) -> list[Path]:
        if not records:
            return []

        ingested = ingested_at or datetime.now(UTC)
        batch = batch_id or f"{ingested:%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"

        by_date: dict[date, list[DeadLetter]] = defaultdict(list)
        for record in records:
            by_date[record.received_at.date()].append(record)

        paths: list[Path] = []
        for received_date, day_records in sorted(by_date.items()):
            rows = [
                {
                    "raw": record.raw,
                    "error": record.error,
                    "received_at": record.received_at,
                    "batch_id": batch,
                }
                for record in day_records
            ]
            path = (
                self.root
                / f"received_date={received_date.isoformat()}"
                / f"batch-{batch}.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_parquet(path, index=False)
            paths.append(path)

        return paths
