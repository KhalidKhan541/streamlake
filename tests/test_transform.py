"""SQL model tests: a small hand-built order lifecycle, transformed for real."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import duckdb

from streamlake.bronze import BronzeWriter
from streamlake.config import Settings
from streamlake.events import EventBase, validate_event
from streamlake.transform import transform

BASE_TIME = datetime(2026, 10, 29, 12, 0, tzinfo=UTC)
INGESTED_AT = datetime(2026, 10, 29, 13, 0, tzinfo=UTC)


def _raw(
    event_id: str,
    event_type: str,
    session_id: str,
    offset_seconds: int,
    **fields: object,
) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_ts": (BASE_TIME + timedelta(seconds=offset_seconds)).isoformat(),
        "user_id": f"u_{session_id}",
        "session_id": session_id,
        **fields,
    }


def _scenario_events() -> list[EventBase]:
    """Three sessions: one paid order, one cancelled order, one bounce."""
    raws = [
        _raw("evt_1", "page_view", "s_1", 0, page="/", device="desktop"),
        _raw(
            "evt_2",
            "add_to_cart",
            "s_1",
            30,
            product_id="P001",
            quantity=2,
            unit_price=10.0,
            currency="USD",
        ),
        _raw(
            "evt_3",
            "order_placed",
            "s_1",
            60,
            order_id="o_1",
            items=[
                {"product_id": "P001", "quantity": 2, "unit_price": 10.0},
                {"product_id": "P002", "quantity": 1, "unit_price": 5.0},
            ],
            total=25.0,
            currency="USD",
        ),
        _raw(
            "evt_4",
            "payment_processed",
            "s_1",
            90,
            order_id="o_1",
            amount=25.0,
            currency="USD",
            method="card",
        ),
        _raw("evt_5", "page_view", "s_2", 300, page="/product", device="mobile"),
        _raw(
            "evt_6",
            "add_to_cart",
            "s_2",
            330,
            product_id="P002",
            quantity=1,
            unit_price=5.0,
            currency="USD",
        ),
        _raw(
            "evt_7",
            "order_placed",
            "s_2",
            360,
            order_id="o_2",
            items=[{"product_id": "P002", "quantity": 1, "unit_price": 5.0}],
            total=5.0,
            currency="USD",
        ),
        _raw(
            "evt_8",
            "order_cancelled",
            "s_2",
            390,
            order_id="o_2",
            reason="customer_request",
        ),
        _raw("evt_9", "page_view", "s_3", 600, page="/", device="tablet"),
    ]
    return [validate_event(raw) for raw in raws]


def _land_bronze(settings: Settings) -> None:
    """Write the scenario twice to simulate an at-least-once redelivery."""
    writer = BronzeWriter(settings.bronze_dir)
    events = _scenario_events()
    writer.write(events, batch_id="batch-1", ingested_at=INGESTED_AT)
    writer.write(events, batch_id="batch-2", ingested_at=INGESTED_AT + timedelta(minutes=1))


def test_models_rebuild_silver_and_gold(settings: Settings) -> None:
    _land_bronze(settings)

    runs = transform(settings)

    assert [run.name for run in runs] == [
        "silver_events",
        "silver_order_items",
        "silver_orders",
        "gold_daily_revenue",
        "gold_funnel",
        "gold_product_performance",
    ]

    conn = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        # Deduplication: nine unique events were delivered twice.
        assert conn.execute("SELECT COUNT(*) FROM analytics.silver_events").fetchone()[0] == 9

        orders = conn.execute(
            """
            SELECT order_id, status, paid_amount, items_total
            FROM analytics.silver_orders
            ORDER BY order_id
            """
        ).fetchall()
        assert orders == [
            ("o_1", "paid", 25.0, 25.0),
            ("o_2", "cancelled", None, 5.0),
        ]

        revenue = conn.execute(
            """
            SELECT
                order_date,
                orders,
                paid_orders,
                cancelled_orders,
                gross_booked,
                net_revenue,
                payment_rate_pct
            FROM analytics.gold_daily_revenue
            """
        ).fetchall()
        assert revenue == [(date(2026, 10, 29), 2, 1, 1, 30.0, 25.0, 50.0)]

        funnel = conn.execute(
            """
            SELECT step, sessions, conversion_pct
            FROM analytics.gold_funnel
            ORDER BY step_order
            """
        ).fetchall()
        assert funnel == [
            ("page_view", 3, 100.0),
            ("add_to_cart", 2, 66.67),
            ("order_placed", 2, 66.67),
            ("payment_processed", 1, 33.33),
        ]

        products = conn.execute(
            """
            SELECT product_id, add_to_cart_events, units, gross_revenue
            FROM analytics.gold_product_performance
            ORDER BY product_id
            """
        ).fetchall()
        assert products == [
            ("P001", 1, 2, 20.0),
            ("P002", 1, 1, 5.0),
        ]

        assert conn.execute("SELECT COUNT(*) FROM analytics.ops_model_runs").fetchone()[0] == 6
    finally:
        conn.close()


def test_rerunning_models_is_idempotent_but_recorded(settings: Settings) -> None:
    _land_bronze(settings)
    transform(settings)
    second_runs = transform(settings)

    assert len(second_runs) == 6

    conn = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM analytics.silver_events").fetchone()[0] == 9
        assert conn.execute("SELECT COUNT(*) FROM analytics.ops_model_runs").fetchone()[0] == 12
    finally:
        conn.close()
