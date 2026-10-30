"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from streamlake.config import Settings

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Isolated lake + warehouse for every test, using the real SQL models."""
    return Settings(
        lake_dir=tmp_path / "lake",
        warehouse_path=tmp_path / "warehouse" / "analytics.duckdb",
        sql_dir=PROJECT_ROOT / "sql",
    )
