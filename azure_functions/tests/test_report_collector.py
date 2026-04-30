"""Tests for dispatch_reports.report_collector — file discovery helpers."""
from __future__ import annotations

import sys
import time
import json
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

    def test_eom_mode_prefers_eom_named_files(self, tmp_path, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "EOM")
        regular = tmp_path / "combined_management_report_2026_20260317_155226.csv"
        eom = tmp_path / "combined_management_report_2026_EOM_20260228_20260310_141323.csv"
        regular.write_text("x")
        time.sleep(0.05)
        eom.write_text("x")
        result = find_files(tmp_path, "combined_management_report_*.csv", 1)
        assert result[0].name == eom.name

    def test_mtd_mode_prefers_non_eom_named_files(self, tmp_path, monkeypatch):
        monkeypatch.setenv("V2_UNIFIED_REFRESH_REPORT_TYPE", "MTD")
        eom = tmp_path / "combined_management_report_2026_EOM_20260228_20260310_141323.csv"
        regular = tmp_path / "combined_management_report_2026_20260317_155226.csv"
        eom.write_text("x")
        time.sleep(0.05)
        regular.write_text("x")
        result = find_files(tmp_path, "combined_management_report_*.csv", 1)
        assert result[0].name == regular.name

    def test_prefers_run_path_artifacts_by_default(self, tmp_path):
        top_level = tmp_path / "combined_management_report_2026_MTD_20260429_v2_20260429_093215.csv"
        run_level = (
            tmp_path
            / "runs"
            / "report_type=MTD"
            / "date=2026-04-29"
            / "run_id=v2_run_2026_MTD_20260429_function-timer_20260429_230825"
            / "combined_management_report_2026_MTD_20260429_function-timer_20260429_230825.csv"
        )
        run_level.parent.mkdir(parents=True, exist_ok=True)
        top_level.write_text("x")
        time.sleep(0.05)
        run_level.write_text("x")
        top_level.touch()

        result = find_files(tmp_path, "combined_management_report_*.csv", 1)
        assert result[0] == run_level

    def test_prefers_blob_last_modified_over_local_mtime(self, tmp_path):
        older_local = tmp_path / "combined_management_report_2026_MTD_20260429_v2_20260429_093215.csv"
        newer_local = tmp_path / "combined_management_report_2026_MTD_20260429_v2_20260429_230825.csv"
        older_local.write_text("a")
        time.sleep(0.05)
        newer_local.write_text("b")

        index_payload = {
            "generated_at_utc": "2026-04-29T21:30:00Z",
            "entries": {
                older_local.relative_to(tmp_path).as_posix(): {"last_modified_epoch": 2000.0},
                newer_local.relative_to(tmp_path).as_posix(): {"last_modified_epoch": 1000.0},
            },
        }
        (tmp_path / _mod._BLOB_INDEX_FILE).write_text(json.dumps(index_payload), encoding="utf-8")

        result = find_files(tmp_path, "combined_management_report_*.csv", 1)
        assert result[0] == older_local


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


# ---------------------------------------------------------------------------
# _check_outputs_freshness
# ---------------------------------------------------------------------------

class TestCheckOutputsFreshness:
    def test_returns_none_when_no_summary_file(self, tmp_path):
        result = _mod._check_outputs_freshness(tmp_path)
        assert result is None

    def test_returns_age_in_hours_when_fresh(self, tmp_path):
        import json
        from datetime import datetime, timezone, timedelta
        recent = datetime.now(timezone.utc) - timedelta(hours=2)
        (tmp_path / "run_summary.json").write_text(
            json.dumps({"generated_at_utc": recent.isoformat()}),
            encoding="utf-8",
        )
        age = _mod._check_outputs_freshness(tmp_path, stale_threshold_hours=26.0)
        assert age is not None
        assert 1.5 < age < 3.0  # should be ~2 hours old

    def test_returns_age_and_warns_when_stale(self, tmp_path):
        import json
        from datetime import datetime, timezone, timedelta
        old_ts = datetime.now(timezone.utc) - timedelta(hours=30)
        (tmp_path / "run_summary.json").write_text(
            json.dumps({"generated_at_utc": old_ts.isoformat()}),
            encoding="utf-8",
        )
        age = _mod._check_outputs_freshness(tmp_path, stale_threshold_hours=26.0)
        assert age is not None
        assert age > 26.0

    def test_returns_none_when_no_date_field(self, tmp_path):
        import json
        (tmp_path / "run_summary.json").write_text(
            json.dumps({"some_other_field": "value"}),
            encoding="utf-8",
        )
        result = _mod._check_outputs_freshness(tmp_path)
        assert result is None


# ---------------------------------------------------------------------------
# derive_report_date
# ---------------------------------------------------------------------------

class TestDeriveReportDate:
    def test_returns_datetime_when_no_csv_present(self, tmp_path):
        import datetime as _dt
        result = _mod.derive_report_date(tmp_path)
        assert isinstance(result, _dt.datetime)

    def test_reads_extract_date_column(self, tmp_path):
        import csv, datetime as _dt
        csv_path = tmp_path / "qry_unified_mapped_20260315_120000.csv"
        with open(csv_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["Extract_Date", "Value"])
            writer.writeheader()
            writer.writerow({"Extract_Date": "2026-03-15 09:00:00", "Value": "100"})
        result = _mod.derive_report_date(tmp_path)
        assert isinstance(result, _dt.datetime)
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 15

    def test_falls_back_to_load_timestamp_when_no_extract_date(self, tmp_path):
        import csv, datetime as _dt
        csv_path = tmp_path / "qry_unified_mapped_20260315_120000.csv"
        with open(csv_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["Load_Timestamp", "Value"])
            writer.writeheader()
            writer.writerow({"Load_Timestamp": "2026-02-28 10:00:00", "Value": "50"})
        result = _mod.derive_report_date(tmp_path)
        assert isinstance(result, _dt.datetime)
        assert result.month == 2


# ---------------------------------------------------------------------------
# build_refresh_command
# ---------------------------------------------------------------------------

class TestBuildRefreshCommand:
    def test_empty_env_var_returns_none(self, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_REFRESH_COMMAND", "")
        result = _mod.build_refresh_command()
        assert result is None

    def test_explicit_command_returned_as_list(self, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_REFRESH_COMMAND", "python generate.py --mode MTD")
        result = _mod.build_refresh_command()
        assert result == ["python", "generate.py", "--mode", "MTD"]

    def test_unset_env_returns_none_or_list(self, monkeypatch):
        monkeypatch.delenv("REPORT_DISPATCH_REFRESH_COMMAND", raising=False)
        result = _mod.build_refresh_command()
        # Either None (if default script missing) or a list (if found)
        assert result is None or isinstance(result, list)


# ---------------------------------------------------------------------------
# resolve_outputs_path
# ---------------------------------------------------------------------------

class TestResolveOutputsPath:
    def test_uses_env_var_when_set(self, tmp_path, monkeypatch):
        monkeypatch.setenv("REPORT_DISPATCH_OUTPUTS_PATH", str(tmp_path))
        result = _mod.resolve_outputs_path()
        assert result == tmp_path

    def test_creates_directory_when_missing(self, tmp_path, monkeypatch):
        target = tmp_path / "new_outputs"
        monkeypatch.setenv("REPORT_DISPATCH_OUTPUTS_PATH", str(target))
        result = _mod.resolve_outputs_path()
        assert result.exists()
