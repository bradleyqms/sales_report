"""Tests for dispatch_reports.eod_digest aggregate helpers."""
from __future__ import annotations

import datetime
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
build_digest_html = _mod.build_digest_html
resolve_digest_date = _mod.resolve_digest_date


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


# ---------------------------------------------------------------------------
# resolve_digest_date
# ---------------------------------------------------------------------------

class TestResolveDigestDate:
    def test_uses_env_override(self, monkeypatch):
        monkeypatch.setenv("EOD_DISPATCH_DIGEST_DATE", "2026-01-15")
        assert resolve_digest_date() == "2026-01-15"

    def test_clears_env_returns_prior_weekday(self, monkeypatch):
        monkeypatch.delenv("EOD_DISPATCH_DIGEST_DATE", raising=False)
        result = resolve_digest_date()
        d = datetime.date.fromisoformat(result)
        assert d.weekday() < 5  # 0=Mon … 4=Fri, never weekend


# ---------------------------------------------------------------------------
# build_digest_html
# ---------------------------------------------------------------------------

def _make_aggregate(health="green", sent=2, failed=0, skipped=0):
    return {
        "health": health,
        "total_events": sent + failed + skipped,
        "totals": {"sent": sent, "failed": failed, "skipped": skipped},
        "by_stream": {"management": {"sent": sent, "failed": failed, "skipped": skipped}},
    }


class TestBuildDigestHtml:
    def test_returns_html_string(self):
        result = build_digest_html("2026-01-14", _make_aggregate(), [])
        assert isinstance(result, str)
        assert "<html>" in result.lower()

    def test_date_token_in_heading(self):
        result = build_digest_html("2026-03-28", _make_aggregate(), [])
        assert "2026-03-28" in result

    def test_green_status_displayed(self):
        result = build_digest_html("2026-01-14", _make_aggregate(health="green"), [])
        assert "GREEN" in result

    def test_amber_status_with_failed_records(self):
        agg = _make_aggregate(health="amber", sent=1, failed=1)
        records = [{"stream": "management", "status": "failed", "error": "graph timeout"}]
        result = build_digest_html("2026-01-14", agg, records)
        assert "AMBER" in result
        assert "graph timeout" in result

    def test_skipped_stream_shows_in_result(self):
        agg = _make_aggregate(health="amber", sent=1, skipped=1)
        records = [{"stream": "core_market", "status": "skipped"}]
        result = build_digest_html("2026-01-14", agg, records)
        assert "skipped" in result.lower()

    def test_no_events_shows_fallback_row(self):
        agg = {"health": "green", "total_events": 0, "totals": {}, "by_stream": {}}
        result = build_digest_html("2026-01-14", agg, [])
        assert "No events recorded" in result


# ---------------------------------------------------------------------------
# aggregate_dispatch_health edge cases
# ---------------------------------------------------------------------------

class TestAggregateDispatchHealthEdgeCases:
    def test_empty_records_returns_green(self):
        agg = aggregate_dispatch_health([])
        assert agg["health"] == "green"
        assert agg["total_events"] == 0

    def test_missing_status_field_treated_as_unknown(self):
        records = [{"stream": "management"}]  # no "status" key
        agg = aggregate_dispatch_health(records)
        assert "unknown" in agg["totals"]
        assert agg["health"] == "green"  # unknown != failed/skipped

    def test_missing_stream_field_groups_as_unknown(self):
        records = [{"status": "sent"}]  # no "stream" key
        agg = aggregate_dispatch_health(records)
        assert "unknown" in agg["by_stream"]

    def test_by_stream_grouping_correct(self):
        records = [
            {"stream": "management", "status": "sent"},
            {"stream": "management", "status": "sent"},
            {"stream": "core_market", "status": "failed"},
        ]
        agg = aggregate_dispatch_health(records)
        assert agg["by_stream"]["management"]["sent"] == 2
        assert agg["by_stream"]["core_market"]["failed"] == 1
        assert agg["health"] == "amber"
