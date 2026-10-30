"""End-to-end demo pipeline test: no Kafka required."""

from __future__ import annotations

import duckdb

from streamlake.config import Settings
from streamlake.demo import run_demo


def test_demo_runs_the_whole_pipeline_and_replays_safely(settings: Settings) -> None:
    first = run_demo(settings, events=400, seed=11, invalid_rate=0.05)

    assert first.generated == 400
    assert first.valid == 380
    assert first.invalid == 20
    assert first.bronze_files
    assert first.dlq_files
    assert len(first.models) == 6

    conn = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM analytics.silver_events").fetchone()[0] == 380
        assert conn.execute("SELECT COUNT(*) FROM analytics.silver_orders").fetchone()[0] > 0
        assert conn.execute("SELECT COUNT(*) FROM analytics.gold_daily_revenue").fetchone()[0] > 0
    finally:
        conn.close()

    # Replaying the exact same event_ids must not inflate the warehouse.
    second = run_demo(settings, events=400, seed=11, invalid_rate=0.05)
    assert second.valid == 380
    assert second.invalid == 20

    conn = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM analytics.silver_events").fetchone()[0] == 380
        assert (
            conn.execute("SELECT COUNT(*) FROM analytics.ops_model_runs").fetchone()[0] == 12
        )
    finally:
        conn.close()
