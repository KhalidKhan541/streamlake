"""Event contract and simulator tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from streamlake.events import generate_events, summarize_validation_error, validate_event


def test_generate_events_is_deterministic() -> None:
    first = generate_events(120, seed=7)
    second = generate_events(120, seed=7)
    other = generate_events(120, seed=8)

    assert first == second
    assert first != other
    assert len(first) == 120


def test_generated_events_satisfy_the_contract() -> None:
    events = generate_events(150, seed=3, reference_time=datetime(2026, 10, 29, 12, tzinfo=UTC))

    validated = [validate_event(event) for event in events]

    assert {event.event_type for event in validated} == {
        "page_view",
        "add_to_cart",
        "order_placed",
        "payment_processed",
        "order_cancelled",
    }
    assert all(event.event_ts.tzinfo is not None for event in validated)
    assert all(event.event_id.startswith("evt_") for event in validated)


def test_invalid_rate_produces_the_expected_dlq_share() -> None:
    events = generate_events(200, seed=11, invalid_rate=0.1)

    valid = 0
    errors: list[str] = []
    for event in events:
        try:
            validate_event(event)
            valid += 1
        except ValidationError as exc:
            errors.append(summarize_validation_error(exc))

    assert valid == 180
    assert len(errors) == 20
    assert all(error for error in errors)


def test_order_total_must_match_line_items() -> None:
    event = {
        "event_id": "evt_1",
        "event_type": "order_placed",
        "event_ts": "2026-10-29T12:00:00+00:00",
        "user_id": "u_1",
        "session_id": "s_1",
        "order_id": "o_1",
        "items": [{"product_id": "P001", "quantity": 2, "unit_price": 10.0}],
        "total": 25.0,
        "currency": "USD",
    }

    with pytest.raises(ValidationError, match="does not match"):
        validate_event(event)


def test_extra_fields_are_forbidden() -> None:
    event = {
        "event_id": "evt_1",
        "event_type": "page_view",
        "event_ts": "2026-10-29T12:00:00+00:00",
        "user_id": "u_1",
        "session_id": "s_1",
        "page": "/",
        "device": "desktop",
        "campaign": "launch",
    }

    with pytest.raises(ValidationError):
        validate_event(event)


def test_naive_timestamps_are_rejected() -> None:
    event = {
        "event_id": "evt_1",
        "event_type": "page_view",
        "event_ts": "2026-10-29T12:00:00",
        "user_id": "u_1",
        "session_id": "s_1",
        "page": "/",
        "device": "desktop",
    }

    with pytest.raises(ValidationError):
        validate_event(event)


def test_unknown_event_types_are_rejected() -> None:
    event = {
        "event_id": "evt_1",
        "event_type": "checkout_started",
        "event_ts": "2026-10-29T12:00:00+00:00",
        "user_id": "u_1",
        "session_id": "s_1",
    }

    with pytest.raises(ValidationError):
        validate_event(event)
