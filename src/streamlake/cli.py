"""Command line interface.

Examples::

    streamlake demo --events 3000 --seed 42
    streamlake produce --rate 40 --duration 30
    streamlake consume --transform-every 2
    streamlake transform
    streamlake query "select * from gold_funnel"
    streamlake stats
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace

import duckdb

from streamlake.config import Settings
from streamlake.consumer import consume_stream
from streamlake.demo import DemoReport, run_demo
from streamlake.producer import produce_stream
from streamlake.transform import NoBronzeDataError, transform
from streamlake.warehouse import (
    WarehouseNotBuiltError,
    connect_readonly,
    recent_model_runs,
    table_counts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="streamlake",
        description="Real-time event streaming into a Parquet + DuckDB lakehouse.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo = subparsers.add_parser("demo", help="run the full pipeline without Kafka")
    demo.add_argument("--events", type=int, default=2000, help="number of events to simulate")
    demo.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    demo.add_argument(
        "--invalid-rate",
        type=float,
        default=0.02,
        help="fraction of events deliberately broken to exercise the DLQ",
    )

    produce = subparsers.add_parser("produce", help="send simulated events to Kafka")
    produce.add_argument("--rate", type=float, default=20.0, help="events per second")
    produce.add_argument("--duration", type=float, default=30.0, help="seconds to produce for")
    produce.add_argument("--seed", type=int, default=None, help="random seed")
    produce.add_argument("--invalid-rate", type=float, default=0.0, help="DLQ demo rate")

    consume = subparsers.add_parser("consume", help="consume Kafka events into the lakehouse")
    consume.add_argument("--batch-size", type=int, default=None, help="max events per flush")
    consume.add_argument(
        "--flush-seconds", type=float, default=None, help="max seconds between flushes"
    )
    consume.add_argument(
        "--transform-every",
        type=int,
        default=0,
        help="rebuild SQL models every N flushes (0 disables)",
    )
    consume.add_argument("--max-messages", type=int, default=None, help="stop after N messages")
    consume.add_argument("--stop-after", type=float, default=None, help="stop after N seconds")

    subparsers.add_parser("transform", help="rebuild the silver/gold SQL models")

    query = subparsers.add_parser("query", help="run SQL against the DuckDB warehouse")
    query.add_argument("sql", help="SQL statement to execute")

    subparsers.add_parser("stats", help="show warehouse table sizes and model run history")

    return parser


def _run_demo(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    report = run_demo(
        settings,
        events=args.events,
        seed=args.seed,
        invalid_rate=args.invalid_rate,
    )
    _print_demo_report(settings, report)
    return 0


def _print_demo_report(settings: Settings, report: DemoReport) -> None:
    print(
        f"generated {report.generated} event(s) | "
        f"valid={report.valid} dead_letter={report.invalid}"
    )
    print(f"bronze files: {len(report.bronze_files)} | dlq files: {len(report.dlq_files)}")

    if not report.models:
        print("no valid events were produced - nothing to transform")
        return

    print("\nmodels:")
    for run in report.models:
        print(f"  {run.name:<28} {run.rows:>7} row(s) {run.duration_ms:>8.1f} ms")

    conn = connect_readonly(settings)
    try:
        revenue = conn.execute(
            "SELECT * FROM analytics.gold_daily_revenue ORDER BY order_date DESC LIMIT 5"
        ).df()
        products = conn.execute(
            "SELECT * FROM analytics.gold_product_performance "
            "ORDER BY gross_revenue DESC LIMIT 5"
        ).df()
    finally:
        conn.close()

    if not revenue.empty:
        print("\nrevenue by day:")
        print(revenue.to_string(index=False))
    if not products.empty:
        print("\ntop products:")
        print(products.to_string(index=False))


def _run_produce(args: argparse.Namespace) -> int:
    produce_stream(
        Settings.from_env(),
        rate=args.rate,
        duration=args.duration,
        seed=args.seed,
        invalid_rate=args.invalid_rate,
    )
    return 0


def _run_consume(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    overrides: dict[str, object] = {}
    if args.batch_size is not None:
        overrides["batch_size"] = args.batch_size
    if args.flush_seconds is not None:
        overrides["flush_interval_seconds"] = args.flush_seconds
    if overrides:
        settings = replace(settings, **overrides)

    consume_stream(
        settings,
        max_messages=args.max_messages,
        transform_every=args.transform_every,
        stop_after=args.stop_after,
    )
    return 0


def _run_transform(_: argparse.Namespace) -> int:
    try:
        runs = transform(Settings.from_env())
    except NoBronzeDataError as exc:
        print(exc, file=sys.stderr)
        return 1

    for run in runs:
        print(f"{run.name:<28} {run.rows:>8} row(s) {run.duration_ms:>9.1f} ms")
    return 0


def _run_query(args: argparse.Namespace) -> int:
    try:
        conn = connect_readonly(Settings.from_env())
    except WarehouseNotBuiltError as exc:
        print(exc, file=sys.stderr)
        return 1

    try:
        frame = conn.execute(args.sql).df()
    except duckdb.Error as exc:
        print(f"query failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    if frame.empty:
        print("(no rows)")
    else:
        print(frame.to_string(index=False))
    return 0


def _run_stats(_: argparse.Namespace) -> int:
    try:
        conn = connect_readonly(Settings.from_env())
    except WarehouseNotBuiltError as exc:
        print(exc, file=sys.stderr)
        return 1

    try:
        print("analytics tables:")
        for name, rows in table_counts(conn):
            print(f"  {name:<30} {rows:>8} row(s)")

        runs = recent_model_runs(conn)
        if runs:
            print("\nlatest model runs:")
            for run_at, model, rows, duration_ms in runs:
                print(
                    f"  {run_at:%Y-%m-%d %H:%M:%S}  {model:<28} "
                    f"{rows:>7} rows  {duration_ms:>8.1f} ms"
                )
    finally:
        conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    args = build_parser().parse_args(argv)
    handlers = {
        "demo": _run_demo,
        "produce": _run_produce,
        "consume": _run_consume,
        "transform": _run_transform,
        "query": _run_query,
        "stats": _run_stats,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
