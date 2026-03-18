from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path


_HERE = Path(__file__).parent
_SPEC = importlib.util.spec_from_file_location(
    "validate_dispatch_dry_run",
    _HERE.parent / "validate_dispatch_dry_run.py",
)
_MOD = importlib.util.module_from_spec(_SPEC)  # type: ignore[arg-type]
_SPEC.loader.exec_module(_MOD)  # type: ignore[union-attr]


def test_selected_modes_defaults_to_both_variants():
    assert _MOD._selected_modes("both") == ["MTD", "EOM"]


def test_selected_modes_accepts_single_mode():
    assert _MOD._selected_modes("MTD") == ["MTD"]
    assert _MOD._selected_modes("eom") == ["EOM"]


def test_expected_end_day_uses_reference_day_for_mtd():
    assert _MOD._expected_end_day(datetime(2026, 3, 16), "MTD") == 16


def test_expected_end_day_uses_full_month_for_eom():
    assert _MOD._expected_end_day(datetime(2026, 2, 28), "EOM") == 28


def test_subject_for_stream_prefixes_only_eom():
    report_date = datetime(2026, 2, 28)
    assert _MOD._subject_for_stream("USA Spa Sales Report", report_date, "fallback", "EOM") == "EOM QMS USA Spa Sales Report 28.02.2026"
    assert _MOD._subject_for_stream("USA Spa Sales Report", report_date, "fallback", "MTD") == "QMS USA Spa Sales Report 28.02.2026"


def test_parse_reference_date_accepts_iso_date():
    result = _MOD._parse_reference_date("2026-03-17")
    assert result == datetime(2026, 3, 17)