"""Tests for dispatch_reports.config — env-var parsing helpers."""
from __future__ import annotations

import os
import sys
from pathlib import Path
import importlib.util

import pytest

_HERE = Path(__file__).parent
_PKG = _HERE.parent / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

_spec = importlib.util.spec_from_file_location(
    "config",
    _HERE.parent / "dispatch_reports" / "config.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)                  # type: ignore[union-attr]

parse_int_env = _mod.parse_int_env
parse_recipients = _mod.parse_recipients
resolve_attachment_patterns = _mod.resolve_attachment_patterns
report_period_label = _mod.report_period_label
report_period_banner = _mod.report_period_banner
report_period_summary = _mod.report_period_summary


class TestParseRecipients:
    def test_none_returns_empty(self):
        assert parse_recipients(None) == []

    def test_empty_string_returns_empty(self):
        assert parse_recipients("") == []

    def test_single_email(self):
        assert parse_recipients("a@b.com") == ["a@b.com"]

    def test_comma_separated(self):
        result = parse_recipients("a@b.com,c@d.com")
        assert result == ["a@b.com", "c@d.com"]

    def test_semicolon_separated(self):
        result = parse_recipients("a@b.com;c@d.com")
        assert result == ["a@b.com", "c@d.com"]

    def test_mixed_separators_with_spaces(self):
        result = parse_recipients("a@b.com ; c@d.com , e@f.com")
        assert result == ["a@b.com", "c@d.com", "e@f.com"]

    def test_trailing_separator_ignored(self):
        result = parse_recipients("a@b.com,")
        assert result == ["a@b.com"]


class TestParseIntEnv:
    def test_missing_returns_default(self, monkeypatch):
        monkeypatch.delenv("TEST_INT_VAR", raising=False)
        assert parse_int_env("TEST_INT_VAR", 42) == 42

    def test_valid_integer(self, monkeypatch):
        monkeypatch.setenv("TEST_INT_VAR", "100")
        assert parse_int_env("TEST_INT_VAR", 42) == 100

    def test_invalid_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("TEST_INT_VAR", "not_a_number")
        assert parse_int_env("TEST_INT_VAR", 42) == 42

    def test_zero_is_valid(self, monkeypatch):
        monkeypatch.setenv("TEST_INT_VAR", "0")
        assert parse_int_env("TEST_INT_VAR", 42) == 0


class TestResolveAttachmentPatterns:
    def test_no_env_returns_empty(self, monkeypatch):
        monkeypatch.delenv("REPORT_DISPATCH_ATTACHMENT_PATTERNS", raising=False)
        monkeypatch.delenv("REPORT_DISPATCH_ATTACHMENTS_PER_PATTERN", raising=False)
        patterns, limit = resolve_attachment_patterns()
        assert patterns == []
        assert limit == 0

    def test_semicolon_split(self, monkeypatch):
        monkeypatch.setenv(
            "REPORT_DISPATCH_ATTACHMENT_PATTERNS",
            "*.csv;report_*.csv",
        )
        monkeypatch.delenv("REPORT_DISPATCH_ATTACHMENTS_PER_PATTERN", raising=False)
        patterns, limit = resolve_attachment_patterns()
        assert patterns == ["*.csv", "report_*.csv"]
        assert limit == 1

    def test_per_pattern_limit_respected(self, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_ATTACHMENT_PATTERNS", "*.csv")
        monkeypatch.setenv("REPORT_DISPATCH_ATTACHMENTS_PER_PATTERN", "3")
        _, limit = resolve_attachment_patterns()
        assert limit == 3


class TestReportPeriodHelpers:
    def test_eom_period_label_uses_full_month(self):
        from datetime import datetime

        result = report_period_label(datetime(2026, 2, 28), "EOM")
        assert result == "EOM: February 1-28, 2026"

    def test_mtd_period_label_uses_reference_day(self):
        from datetime import datetime

        result = report_period_label(datetime(2026, 3, 16), "MTD")
        assert result == "MTD: March 1-16, 2026"

    def test_report_period_banner_wraps_label(self):
        from datetime import datetime

        result = report_period_banner("USA Spa Sales Report", datetime(2026, 2, 28), "EOM")
        assert result == "USA Spa Sales Report (EOM: February 1-28, 2026)"

    def test_report_period_summary_switches_by_mode(self):
        assert report_period_summary(mode="EOM") == "End-of-month figures"
        assert report_period_summary(mode="MTD") == "Month-to-date figures"
