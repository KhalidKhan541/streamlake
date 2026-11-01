# Architecture

`streamlake` is a small real-time lakehouse. The streaming path and the demo
path share the same validation, bronze and SQL code; the only difference is
where the JSON comes from (Kafka or the simulator).

## Components

| Component | File | Responsibility |
| --- | --- | --- |
| Event contracts | `src/streamlake/events.py` | pydantic v2 models + deterministic simulator |
| Producer | `src/streamlake/producer.py` | simulate a clickstream and publish JSON to Kafka |
| Consumer | `src/streamlake/consumer.py` | validate, micro-batch, land Parquet, commit offsets |
| Bronze / DLQ | `src/streamlake/bronze.py` | immutable Parquet writers |
| Transform | `src/streamlake/transform.py` | DuckDB SQL model runner + run audit |
| Warehouse helpers | `src/streamlake/warehouse.py` | read-only stats and model-run queries |
| Demo | `src/streamlake/demo.py` | Kafka-free end-to-end run |
| CLI | `src/streamlake/cli.py` | `demo`, `produce`, `consume`, `transform`, `query`, `stats` |

## Streaming flow

```text
producer ──JSON──> Kafka topic "events"
                        │
                        ▼
                  consumer.poll()
                        │
              ┌─────────┴──────────┐
              ▼                    ▼
        valid events         invalid records
              │                    │
              ▼                    ▼
   lake/bronze/events/...   lake/dlq/...
   event_date=YYYY-MM-DD    received_date=YYYY-MM-DD
              │                    │
              └─────────┬──────────┘
                        ▼
             commit Kafka offsets (sync)
                        │
             every N flushes (optional)
                        ▼
               DuckDB SQL models
```

The consumer buffers messages. A batch is flushed when either
`STREAMLAKE_BATCH_SIZE` messages are buffered or
`STREAMLAKE_FLUSH_SECONDS` have elapsed since the previous flush, whichever
comes first. Flushes also happen on shutdown.

## Delivery semantics

The pipeline deliberately chooses **at-least-once ingestion with idempotent
processing** instead of relying on Kafka transactions:

1. `enable.auto.commit=false`; offsets are committed manually.
2. The consumer validates a batch and writes it to Parquet. Both the bronze
   file and the DLQ file are closed before the next step.
3. Only after those writes succeed does the consumer call `commit()`.
4. If the process dies before the commit, Kafka redelivers the batch. The
   duplicate bronze rows are harmless because `010_silver_events.sql` keeps one
   row per `event_id` (`ROW_NUMBER() ... WHERE duplicate_rank = 1`).
5. If the process dies after the commit, no data is lost; the batch is already
   on disk.

The result is **effectively-once data in the warehouse**, even though delivery
is at-least-once. Consumers never ack a message they have not durably stored.

### Why not producer transactions?

Kafka transactions give exactly-once between Kafka topics, but the sink here is
a filesystem/DuckDB, which is not transactional with Kafka. Committing only
after a durable write and deduplicating on an immutable business key (`event_id`)
is simpler and robust across crashes, restarts and consumer-group rebalances.

## Storage layout

```text
lake/
├── bronze/
│   └── events/
│       └── event_date=2026-10-30/
│           └── batch-20261030T112405-4f2a91c3.parquet
└── dlq/
    └── received_date=2026-10-30/
        └── batch-20261030T112405-4f2a91c3.parquet
warehouse/
└── streamlake.duckdb
```

Bronze columns:

| Column | Type | Notes |
| --- | --- | --- |
| `event_id` | string | business key used for deduplication |
| `event_type` | string | discriminated union tag |
| `event_ts` | timestamp (UTC) | event time, not ingestion time |
| `user_id`, `session_id` | string | envelope fields |
| `payload` | JSON | type-specific fields only |
| `ingested_at` | timestamp (UTC) | when the consumer received the batch |
| `batch_id` | string | trace from a row back to its micro-batch/file |

DLQ columns: `raw` (original message), `error` (one-line validation error),
`received_at`, `batch_id`. DLQ records are never dropped automatically; replay
them after fixing the producer or contract.

## Model DAG

```text
bronze_events (view over Parquet)
    │
    ├── 010_silver_events          dedupe + typed JSON extraction
    │       ├── 020_silver_order_items   UNNEST(order_placed.items)
    │       └── 030_silver_orders        paid / pending / cancelled lifecycle
    │
    ├── 040_gold_daily_revenue     grain: order_date
    ├── 050_gold_funnel            grain: session_date × funnel step
    └── 060_gold_product_performance  grain: product_id
```

Every file is a single `CREATE OR REPLACE TABLE analytics.<model>` statement and
runs in filename order. `analytics.ops_model_runs` records the row count and
duration of every model on every run. Gold tables are fully rebuilt, so a bug
fix in SQL is one `streamlake transform` away.

### Order lifecycle rules

`030_silver_orders.sql` groups by `order_id` and derives status:

- `paid` if a `payment_processed` event exists,
- otherwise `cancelled` if an `order_cancelled` event exists,
- otherwise `pending`.

Revenue counts only paid orders; cancelled and pending orders remain visible in
`gross_booked` and the status counts. Payment and cancellation events for an
unknown order are ignored by the `placed` base table (they cannot be joined),
which mirrors the fact that an order must exist before it can be paid.

## Failure modes

| Failure | Behaviour |
| --- | --- |
| Bad JSON / schema violation | Record goes to DLQ; the batch continues |
| Crash before offset commit | Kafka redelivers; silver dedupes |
| Crash after offset commit | Bronze is already durable; no loss |
| Transform failure | Logged; bronze is intact and the next cycle retries |
| Duplicate producer retry | Same `event_id` collapses in silver |
| Late/out-of-order events | Accepted; event time drives dates and ordering |
| Warehouse deleted | Rebuilt from bronze with `streamlake transform` |

## Operational notes

- **Single-writer DuckDB.** One consumer process owns the warehouse while it is
  transforming. Use read-only queries for inspection. For higher concurrency,
  move gold tables to ClickHouse/BigQuery and keep SQL semantics.
- **Partitioning by event date** keeps files roughly diurnal. A 24h lookback
  means a batch can land in two partitions; the writer creates one file per
  date per batch.
- **Batch file naming** includes ingestion time and a random suffix, so files
  are immutable and replay-safe: a redelivery creates a new file, never edits an
  old one.
- **Scaling path**: replace the local filesystem with object storage, keep
  `event_id` as the merge key, add a table format (Iceberg/Delta) for
  manifests and compaction, and use Kafka partitions by `session_id` to
  preserve per-session ordering.
