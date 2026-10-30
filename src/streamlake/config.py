"""Application configuration, resolved from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Repo root inferred from this file: <root>/src/streamlake/config.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    """All tunables of the platform in one place."""

    lake_dir: Path
    warehouse_path: Path
    sql_dir: Path
    kafka_bootstrap: str = "localhost:29092"
    topic: str = "events"
    group_id: str = "streamlake-ingest"
    batch_size: int = 500
    flush_interval_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            lake_dir=Path(os.getenv("STREAMLAKE_LAKE_DIR", str(PROJECT_ROOT / "lake"))),
            warehouse_path=Path(
                os.getenv(
                    "STREAMLAKE_WAREHOUSE",
                    str(PROJECT_ROOT / "warehouse" / "analytics.duckdb"),
                )
            ),
            sql_dir=Path(os.getenv("STREAMLAKE_SQL_DIR", str(PROJECT_ROOT / "sql"))),
            kafka_bootstrap=os.getenv("STREAMLAKE_KAFKA_BOOTSTRAP", "localhost:29092"),
            topic=os.getenv("STREAMLAKE_TOPIC", "events"),
            group_id=os.getenv("STREAMLAKE_GROUP_ID", "streamlake-ingest"),
            batch_size=int(os.getenv("STREAMLAKE_BATCH_SIZE", "500")),
            flush_interval_seconds=float(os.getenv("STREAMLAKE_FLUSH_SECONDS", "5")),
        )

    @property
    def bronze_dir(self) -> Path:
        return self.lake_dir / "bronze" / "events"

    @property
    def dlq_dir(self) -> Path:
        return self.lake_dir / "dlq"
