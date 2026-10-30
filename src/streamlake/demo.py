"""Kafka-free end-to-end pipeline used by the demo command and tests.

The flow mirrors the streaming path exactly (validate -> bronze/DLQ -> SQL
models), so the demo is a faithful preview of production behaviour.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from streamlake.bronze import BronzeWriter, DeadLetter, DeadLetterWriter
from streamlake.config import Settings
from streamlake.events import generate_events, summarize_validation_error, validate_event
from streamlake.transform import ModelRun, transform


@dataclass(frozen=True)
class DemoReport:
    generated: int
    valid: int
    invalid: int
    bronze_files: list[Path]
    dlq_files: list[Path]
    models: list[ModelRun]


def run_demo(
    settings: Settings,
    events: int = 2000,
    seed: int = 42,
    invalid_rate: float = 0.02,
    reference_time: datetime | None = None,
) -> DemoReport:
    """Generate events, validate them, land bronze/DLQ files and transform."""
    reference = reference_time or datetime.now(UTC)
    raw_events = generate_events(
        events,
        seed=seed,
        reference_time=reference,
        invalid_rate=invalid_rate,
    )
    received_at = datetime.now(UTC)

    valid_events = []
    dead_letters: list[DeadLetter] = []
    for raw in raw_events:
        try:
            valid_events.append(validate_event(raw))
        except ValidationError as exc:
            dead_letters.append(
                DeadLetter(
                    raw=json.dumps(raw, sort_keys=True),
                    error=summarize_validation_error(exc),
                    received_at=received_at,
                )
            )

    bronze_files = BronzeWriter(settings.bronze_dir).write(
        valid_events, ingested_at=received_at
    )
    dlq_files = DeadLetterWriter(settings.dlq_dir).write(dead_letters, ingested_at=received_at)

    models = transform(settings) if valid_events else []

    return DemoReport(
        generated=len(raw_events),
        valid=len(valid_events),
        invalid=len(dead_letters),
        bronze_files=bronze_files,
        dlq_files=dlq_files,
        models=models,
    )
