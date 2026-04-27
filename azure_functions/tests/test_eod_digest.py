"""Tests for dispatch_reports.eod_digest aggregate helpers."""
from __future__ import annotations

import importlib.util
from pathlib import Path

_HERE = Path(__file__).parent

_spec = importlib.util.spec_from_file_location(
    "eod_digest",
    _HERE.parent / "dispatch_reports" / "eod_digest.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

aggregate_dispatch_health = _mod.aggregate_dispatch_health


def test_aggregate_dispatch_health_green_when_only_sent():
    aggregate = aggregate_dispatch_health(
        [
            {"stream": "management", "status": "sent"},
            {"stream": "core_market", "status": "sent"},
        ]
    )
    assert aggregate["health"] == "green"
    assert aggregate["totals"]["sent"] == 2


def test_aggregate_dispatch_health_amber_when_failed_or_skipped():
    aggregate = aggregate_dispatch_health(
        [
            {"stream": "management", "status": "sent"},
            {"stream": "core_market", "status": "failed"},
            {"stream": "usa_spa", "status": "skipped"},
        ]
    )
    assert aggregate["health"] == "amber"
    assert aggregate["totals"]["failed"] == 1
    assert aggregate["totals"]["skipped"] == 1
