"""Tests for dispatch_reports.report_collector — file discovery helpers."""
from __future__ import annotations

import sys
import time
from pathlib import Path
import importlib.util

import pytest

_HERE = Path(__file__).parent
_PKG = _HERE.parent / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

# Load config first so report_collector's relative import can be satisfied
_cfg_spec = importlib.util.spec_from_file_location(
    "dispatch_reports.config",
    _HERE.parent / "dispatch_reports" / "config.py",
)
_cfg_mod = importlib.util.module_from_spec(_cfg_spec)  # type: ignore[arg-type]
sys.modules["dispatch_reports.config"] = _cfg_mod
sys.modules["dispatch_reports"] = type(sys)("dispatch_reports")  # stub package
_cfg_spec.loader.exec_module(_cfg_mod)  # type: ignore[union-attr]

_spec = importlib.util.spec_from_file_location(
    "dispatch_reports.report_collector",
    _HERE.parent / "dispatch_reports" / "report_collector.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["dispatch_reports.report_collector"] = _mod
_spec.loader.exec_module(_mod)                  # type: ignore[union-attr]

find_files = _mod.find_files
collect_html_files = _mod.collect_html_files
collect_csv_attachments = _mod.collect_csv_attachments


class TestFindFiles:
    def test_empty_dir_returns_empty(self, tmp_path):
        assert find_files(tmp_path, "*.html", 5) == []

    def test_missing_dir_returns_empty(self, tmp_path):
        missing = tmp_path / "missing"
        assert find_files(missing, "*.html", 5) == []

    def test_finds_matching_files(self, tmp_path):
        (tmp_path / "a.html").write_text("x")
        (tmp_path / "b.html").write_text("x")
        result = find_files(tmp_path, "*.html", 10)
        assert len(result) == 2

    def test_limit_enforced(self, tmp_path):
        for i in range(5):
            (tmp_path / f"r_{i}.html").write_text("x")
        result = find_files(tmp_path, "*.html", 2)
        assert len(result) == 2

    def test_returns_newest_first(self, tmp_path):
        old = tmp_path / "old.html"
        new = tmp_path / "new.html"
        old.write_text("x")
        time.sleep(0.05)
        new.write_text("x")
        result = find_files(tmp_path, "*.html", 2)
        assert result[0].name == "new.html"

    def test_no_match_returns_empty(self, tmp_path):
        (tmp_path / "report.csv").write_text("x")
        assert find_files(tmp_path, "*.html", 5) == []


class TestCollectHtmlFiles:
    def test_returns_newest_per_pattern(self, tmp_path, monkeypatch):
        # Patch KEY_HTML_PATTERNS to use our tmp fixtures
        monkeypatch.setattr(_mod, "KEY_HTML_PATTERNS", ["combined_*.html", "core_*.html"])
        (tmp_path / "combined_old.html").write_text("x")
        time.sleep(0.05)
        (tmp_path / "combined_new.html").write_text("x")
        (tmp_path / "core_markets.html").write_text("x")
        result = collect_html_files(tmp_path)
        names = [p.name for p in result]
        assert "combined_new.html" in names
        assert "core_markets.html" in names
        assert "combined_old.html" not in names

    def test_no_duplicates(self, tmp_path, monkeypatch):
        # Both patterns match the same file → should appear only once
        monkeypatch.setattr(_mod, "KEY_HTML_PATTERNS", ["report*.html", "report*.html"])
        (tmp_path / "report_2026.html").write_text("x")
        result = collect_html_files(tmp_path)
        assert len(result) == 1

    def test_empty_dir_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_mod, "KEY_HTML_PATTERNS", ["*.html"])
        assert collect_html_files(tmp_path) == []


class TestCollectCsvAttachments:
    def test_falls_back_to_key_patterns(self, tmp_path, monkeypatch):
        monkeypatch.delenv("REPORT_DISPATCH_ATTACHMENT_PATTERNS", raising=False)
        monkeypatch.delenv("REPORT_DISPATCH_ATTACHMENTS_PER_PATTERN", raising=False)
        monkeypatch.setattr(_mod, "KEY_CSV_PATTERNS", ["report_*.csv"])
        (tmp_path / "report_2026.csv").write_text("a,b")
        result = collect_csv_attachments(tmp_path)
        assert len(result) == 1

    def test_env_patterns_used_when_set(self, tmp_path, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_ATTACHMENT_PATTERNS", "data_*.csv")
        monkeypatch.setenv("REPORT_DISPATCH_ATTACHMENTS_PER_PATTERN", "1")
        (tmp_path / "data_q1.csv").write_text("a,b")
        (tmp_path / "data_q2.csv").write_text("a,b")
        result = collect_csv_attachments(tmp_path)
        assert len(result) == 1  # per_limit=1 per pattern

    def test_no_duplicates(self, tmp_path, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_ATTACHMENT_PATTERNS", "*.csv;*.csv")
        (tmp_path / "single.csv").write_text("a")
        result = collect_csv_attachments(tmp_path)
        assert len(result) == 1

    def test_non_csv_env_patterns_ignored_in_csv_collection(self, tmp_path, monkeypatch):
        # When patterns contain only HTML globs, falls back to KEY_CSV_PATTERNS
        monkeypatch.setenv("REPORT_DISPATCH_ATTACHMENT_PATTERNS", "*.html")
        monkeypatch.setattr(_mod, "KEY_CSV_PATTERNS", ["report_*.csv"])
        (tmp_path / "report_2026.csv").write_text("a,b")
        result = collect_csv_attachments(tmp_path)
        # HTML-only patterns filtered out -> fallback to KEY_CSV_PATTERNS
        assert len(result) == 1
