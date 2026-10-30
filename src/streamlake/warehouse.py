"""Read-only helpers for querying the DuckDB warehouse."""

from __future__ import annotations

import duckdb

from streamlake.config import Settings


class WarehouseNotBuiltError(RuntimeError):
    """Raised when the warehouse file does not exist yet."""


def connect_readonly(settings: Settings) -> duckdb.DuckDBPyConnection:
    if not settings.warehouse_path.exists():
        raise WarehouseNotBuiltError(
            f"warehouse not found at {settings.warehouse_path}; "
            "run `streamlake demo`, `streamlake transform`, or start the consumer first"
        )
    return duckdb.connect(str(settings.warehouse_path), read_only=True)


def table_counts(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, int]]:
    """Row counts for every table in the analytics schema."""
    names = conn.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'analytics'
        order by table_name
        """
    ).fetchall()

    counts: list[tuple[str, int]] = []
    for (name,) in names:
        rows = conn.execute(f'select count(*) from analytics."{name}"').fetchone()[0]
        counts.append((name, int(rows)))
    return counts


def recent_model_runs(
    conn: duckdb.DuckDBPyConnection, limit: int = 10
) -> list[tuple[object, str, int, float]]:
    """Latest rows from the ops_model_runs audit table."""
    return conn.execute(
        """
        select run_at, model, rows, duration_ms
        from analytics.ops_model_runs
        order by run_at desc
        limit ?
        """,
        [limit],
    ).fetchall()
