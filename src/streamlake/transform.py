"""DuckDB SQL model runner.

Each file in ``sql/`` is one statement. Files are executed in filename order
(hence the numeric prefixes) and every run is recorded in
``analytics.ops_model_runs``.

The runner exposes a ``bronze_events`` view over the Parquet bronze files
before executing anything, so models never need to know filesystem paths.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from streamlake.config import Settings

LOGGER = logging.getLogger(__name__)


class NoBronzeDataError(RuntimeError):
    """Raised when there is nothing to transform yet."""


@dataclass(frozen=True)
class ModelRun:
    name: str
    rows: int
    duration_ms: float


def transform(settings: Settings) -> list[ModelRun]:
    """Rebuild all silver and gold models from the bronze layer."""
    bronze_files = list(settings.bronze_dir.rglob("*.parquet"))
    if not bronze_files:
        raise NoBronzeDataError(
            f"no bronze data under {settings.bronze_dir}; "
            "run `streamlake demo` or send events through the consumer first"
        )

    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    glob = (settings.bronze_dir / "**" / "*.parquet").as_posix().replace("'", "''")

    conn = duckdb.connect(str(settings.warehouse_path))
    try:
        conn.execute("SET TimeZone='UTC'")
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        conn.execute(
            f"CREATE OR REPLACE VIEW bronze_events AS "
            f"SELECT * FROM read_parquet('{glob}', union_by_name = true)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics.ops_model_runs (
                run_at TIMESTAMPTZ,
                model VARCHAR,
                rows BIGINT,
                duration_ms DOUBLE
            )
            """
        )

        runs: list[ModelRun] = []
        for path in sorted(settings.sql_dir.glob("*.sql")):
            if "_" not in path.stem:
                LOGGER.warning("skipping SQL file without a numeric prefix: %s", path.name)
                continue

            name = path.stem.split("_", 1)[1]
            started = time.perf_counter()
            conn.execute(path.read_text(encoding="utf-8"))
            rows = int(conn.execute(f'SELECT COUNT(*) FROM analytics."{name}"').fetchone()[0])
            duration_ms = round((time.perf_counter() - started) * 1000, 2)

            conn.execute(
                "INSERT INTO analytics.ops_model_runs VALUES (?, ?, ?, ?)",
                [datetime.now(UTC), name, rows, duration_ms],
            )
            runs.append(ModelRun(name=name, rows=rows, duration_ms=duration_ms))
            LOGGER.info("model %-28s %7d row(s) %8.1f ms", name, rows, duration_ms)

        return runs
    finally:
        conn.close()


def query_rows(settings: Settings, sql: str) -> list[tuple]:
    """Run an ad-hoc SQL statement and return raw rows."""
    conn = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()
