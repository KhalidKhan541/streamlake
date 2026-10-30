"""Event contracts and a deterministic clickstream simulator.

The pydantic models are the single source of truth for what a valid event is:
the producer generates them, the consumer validates them, and bronze stores
their JSON. A discriminated union on ``event_type`` keeps validation exact.

The simulator makes demos and tests deterministic: the same ``(count, seed,
reference_time, invalid_rate)`` always produces the same events.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    model_validator,
)

PAGES: tuple[str, ...] = ("/", "/search", "/product", "/cart", "/checkout", "/account")
DEVICES: tuple[str, ...] = ("desktop", "mobile", "tablet")
PAYMENT_METHODS: tuple[str, ...] = ("card", "paypal", "apple_pay")
CANCEL_REASONS: tuple[str, ...] = ("customer_request", "out_of_stock", "payment_failed")
CURRENCY = "USD"
USER_COUNT = 200

# Small, stable product catalog: product_id -> unit price.
PRODUCT_PRICES: dict[str, float] = {
    "P001": 19.99,
    "P002": 49.50,
    "P003": 7.25,
    "P004": 129.00,
    "P005": 15.00,
    "P006": 89.99,
    "P007": 5.50,
    "P008": 24.00,
    "P009": 399.00,
    "P010": 12.75,
}

# Fields that live in dedicated bronze columns rather than in the payload JSON.
ENVELOPE_FIELDS = frozenset({"event_id", "event_type", "event_ts", "user_id", "session_id"})


class EventBase(BaseModel):
    """Fields every event carries, regardless of type."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1)
    event_ts: AwareDatetime
    user_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)

    def payload_json(self) -> str:
        """Type-specific fields as JSON, ready for the bronze ``payload`` column."""
        return self.model_dump_json(exclude=ENVELOPE_FIELDS)


class PageView(EventBase):
    event_type: Literal["page_view"]
    page: str = Field(min_length=1)
    device: Literal["desktop", "mobile", "tablet"]


class AddToCart(EventBase):
    event_type: Literal["add_to_cart"]
    product_id: str = Field(min_length=1)
    quantity: int = Field(gt=0, le=10)
    unit_price: float = Field(gt=0)
    currency: str = "USD"


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    product_id: str = Field(min_length=1)
    quantity: int = Field(gt=0, le=10)
    unit_price: float = Field(gt=0)


class OrderPlaced(EventBase):
    event_type: Literal["order_placed"]
    order_id: str = Field(min_length=1)
    items: list[OrderItem] = Field(min_length=1)
    total: float = Field(gt=0)
    currency: str = "USD"

    @model_validator(mode="after")
    def _total_matches_items(self) -> OrderPlaced:
        computed = round(sum(item.quantity * item.unit_price for item in self.items), 2)
        if abs(computed - self.total) > 0.01:
            raise ValueError(f"total {self.total} does not match item sum {computed}")
        return self


class PaymentProcessed(EventBase):
    event_type: Literal["payment_processed"]
    order_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    currency: str = "USD"
    method: Literal["card", "paypal", "apple_pay"]


class OrderCancelled(EventBase):
    event_type: Literal["order_cancelled"]
    order_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)


Event = Annotated[
    PageView | AddToCart | OrderPlaced | PaymentProcessed | OrderCancelled,
    Field(discriminator="event_type"),
]

EventAdapter = TypeAdapter(Event)


def validate_event(raw: dict) -> EventBase:
    """Validate a decoded event payload, raising ``ValidationError`` if invalid."""
    return EventAdapter.validate_python(raw)


def summarize_validation_error(exc: ValidationError) -> str:
    """One-line error summary suitable for the DLQ ``error`` column."""
    first = exc.errors()[0]
    location = ".".join(str(part) for part in first.get("loc", ())) or "event"
    return f"{location}: {first.get('msg', 'invalid event')}"


def generate_events(
    count: int,
    seed: int = 42,
    reference_time: datetime | None = None,
    lookback: timedelta | None = None,
    invalid_rate: float = 0.0,
) -> list[dict]:
    """Simulate sessions moving through a clickstream -> checkout funnel.

    Returns JSON-ready dicts (not models) so callers can choose to send them
    over Kafka or validate them in-process.
    """
    if count < 0:
        raise ValueError("count must be >= 0")
    if not 0.0 <= invalid_rate < 1.0:
        raise ValueError("invalid_rate must be in [0, 1)")

    reference = reference_time or datetime.now(UTC)
    window = lookback if lookback is not None else timedelta(hours=24)
    rng = random.Random(seed)
    products = list(PRODUCT_PRICES)
    events: list[dict] = []
    counter = 0

    def next_event_id() -> str:
        nonlocal counter
        counter += 1
        return f"evt_{seed:x}_{counter:08d}"

    session_seq = 0
    while len(events) < count:
        session_seq += 1
        session_id = f"s_{seed:x}_{session_seq:06d}"
        user_id = f"u_{rng.randrange(USER_COUNT):04d}"
        ts = reference - timedelta(seconds=rng.uniform(0, window.total_seconds()))
        ts = ts.replace(microsecond=0)

        for _ in range(rng.randint(1, 5)):
            ts += timedelta(seconds=rng.randint(3, 75))
            events.append(
                {
                    "event_id": next_event_id(),
                    "event_type": "page_view",
                    "event_ts": ts.isoformat(),
                    "user_id": user_id,
                    "session_id": session_id,
                    "page": rng.choice(PAGES),
                    "device": rng.choice(DEVICES),
                }
            )

        if rng.random() < 0.45:
            for _ in range(rng.randint(1, 3)):
                ts += timedelta(seconds=rng.randint(5, 120))
                product_id = rng.choice(products)
                events.append(
                    {
                        "event_id": next_event_id(),
                        "event_type": "add_to_cart",
                        "event_ts": ts.isoformat(),
                        "user_id": user_id,
                        "session_id": session_id,
                        "product_id": product_id,
                        "quantity": rng.randint(1, 3),
                        "unit_price": PRODUCT_PRICES[product_id],
                        "currency": CURRENCY,
                    }
                )

        if rng.random() < 0.30:
            ts += timedelta(seconds=rng.randint(10, 180))
            order_id = f"ord_{seed:x}_{session_seq:06d}"
            items = []
            for _ in range(rng.randint(1, 3)):
                product_id = rng.choice(products)
                items.append(
                    {
                        "product_id": product_id,
                        "quantity": rng.randint(1, 2),
                        "unit_price": PRODUCT_PRICES[product_id],
                    }
                )
            total = round(sum(item["quantity"] * item["unit_price"] for item in items), 2)
            events.append(
                {
                    "event_id": next_event_id(),
                    "event_type": "order_placed",
                    "event_ts": ts.isoformat(),
                    "user_id": user_id,
                    "session_id": session_id,
                    "order_id": order_id,
                    "items": items,
                    "total": total,
                    "currency": CURRENCY,
                }
            )

            outcome = rng.random()
            if outcome < 0.72:
                ts += timedelta(seconds=rng.randint(5, 120))
                events.append(
                    {
                        "event_id": next_event_id(),
                        "event_type": "payment_processed",
                        "event_ts": ts.isoformat(),
                        "user_id": user_id,
                        "session_id": session_id,
                        "order_id": order_id,
                        "amount": total,
                        "currency": CURRENCY,
                        "method": rng.choice(PAYMENT_METHODS),
                    }
                )
            elif outcome < 0.82:
                ts += timedelta(seconds=rng.randint(5, 120))
                events.append(
                    {
                        "event_id": next_event_id(),
                        "event_type": "order_cancelled",
                        "event_ts": ts.isoformat(),
                        "user_id": user_id,
                        "session_id": session_id,
                        "order_id": order_id,
                        "reason": rng.choice(CANCEL_REASONS),
                    }
                )

    events = events[:count]

    if invalid_rate > 0:
        invalid_count = min(len(events), int(count * invalid_rate))
        for index in rng.sample(range(len(events)), invalid_count):
            events[index] = _corrupt(events[index], rng)

    return events


def _corrupt(event: dict, rng: random.Random) -> dict:
    """Return a deliberately invalid copy of an event (exercises the DLQ path)."""
    corrupted = dict(event)
    kind = rng.choice(("missing_field", "unknown_type", "negative_quantity", "bad_timestamp"))
    if kind == "missing_field":
        corrupted.pop("session_id", None)
    elif kind == "unknown_type":
        corrupted["event_type"] = "unknown_event"
    elif kind == "negative_quantity":
        corrupted["quantity"] = -1
    else:
        corrupted["event_ts"] = "not-a-timestamp"
    return corrupted
