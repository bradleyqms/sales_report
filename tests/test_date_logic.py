"""
Unit tests for DNR-55 date-logic fix.

Verifies that every date used in report generation (headers, budget filtering,
prior-year filtering) derives from the SAP Extract_Date anchor rather than the
system clock.
"""

import datetime
import io
import logging
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
SRC = Path(__file__).parent.parent / "src"
AZURE = Path(__file__).parent.parent / "azure_functions"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(AZURE))

from utils import get_current_month, get_current_year, get_prior_year

# ============================================================
# Step 1 — utils.py date helper tests
# ============================================================

REF_FEB = datetime.datetime(2026, 2, 27)
REF_MAR = datetime.datetime(2026, 3, 2)


def test_get_current_month_with_reference():
    assert get_current_month(REF_FEB) == 2


def test_get_current_month_ignores_system_clock():
    """Even if the system clock says March, the reference date wins."""
    with patch("utils.datetime.datetime") as mock_dt:
        mock_dt.now.return_value = REF_MAR
        result = get_current_month(REF_FEB)
    assert result == 2, "Expected February (2), not March (3)"


def test_get_prior_year_with_reference():
    assert get_prior_year(REF_FEB) == 2025


def test_get_current_year_with_reference():
    assert get_current_year(REF_FEB) == 2026


def test_get_current_month_no_reference_uses_now():
    """Without a reference, get_current_month() returns the real current month."""
    expected = datetime.datetime.now().month
    assert get_current_month() == expected


# ============================================================
# Step 2 — BaseReportGenerator._prepare_dates
# ============================================================

def _make_minimal_csvs(tmp_path: Path):
    """Write the three minimal CSVs required to instantiate any report generator."""
    sales = tmp_path / "sales.csv"
    budget = tmp_path / "budget.csv"
    prior = tmp_path / "prior.csv"

    # Sales CSV — minimal columns expected by GVLReportGenerator._prepare_data
    pd.DataFrame({
        "Sales Employee Name": ["Alice"],
        "Customer Name": ["Cust A"],
        "Total Value (EUR)": [1000.0],
        "Document Type": ["AR"],
        "Company Entity": ["GmbH"],
        "Currency": ["EUR"],
        "Source_File": ["QRY_AR_MTD_Gmbh.csv"],
        "Load_Timestamp": [pd.Timestamp("2026-02-28")],
        "Customer Code": [None],
        "Total Open Value (EUR)": [1000.0],
        "Value_in_EUR_converted": [1000.0],
        "Market_Group": ["Germany"],
        "Region": ["Germany"],
        "Channel_Level": ["Direct"],
    }).to_csv(sales, index=False)

    # Budget CSV — one February row and one March row (with GVL-required column)
    pd.DataFrame({
        "Date": ["01/02/2026", "01/03/2026"],
        "Sales Employee / Account": ["Alice", "Alice"],
        "Market_Group": ["Germany", "Germany"],
        "Value_kEUR": [100.0, 120.0],
        "kEUR": [100.0, 120.0],
    }).to_csv(budget, index=False)

    # Prior year CSV (same schema)
    pd.DataFrame({
        "Date": ["01/02/2025", "01/03/2025"],
        "Sales Employee / Account": ["Alice", "Alice"],
        "Market_Group": ["Germany", "Germany"],
        "Value_kEUR": [90.0, 110.0],
        "kEUR": [90.0, 110.0],
    }).to_csv(prior, index=False)

    return str(sales), str(budget), str(prior)


def _gvl_config_path() -> str:
    cfg = SRC / "config" / "gvl_report_structure.json"
    if cfg.exists():
        return str(cfg)
    # Minimal fallback JSON if the real config is not in the test environment
    import json, tempfile as _tmp
    data = {
        "report_groups": [
            {
                "label": "Germany",
                "market_group": "Germany",
                "region": "Germany",
                "channel": "Direct",
                "is_total": False,
            }
        ],
        "currency": "EUR",
    }
    fd, path = _tmp.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    return path


@pytest.fixture
def gvl_generator(tmp_path):
    """Return a GVLReportGenerator anchored to 2026-02-28."""
    from gvl_report import GVLReportGenerator

    sales, budget, prior = _make_minimal_csvs(tmp_path)
    cfg = _gvl_config_path()
    return GVLReportGenerator(
        cfg, sales, budget, prior,
        report_date=datetime.datetime(2026, 2, 28),
    )


def test_prepare_dates_uses_report_date(gvl_generator):
    gen = gvl_generator
    assert gen.current_month == 2, f"Expected month 2 (Feb), got {gen.current_month}"
    assert gen.now.month == 2, f"Expected self.now.month == 2, got {gen.now.month}"
    assert gen.current_year == 2026
    assert gen.prior_year == 2025


def test_budget_filter_uses_report_date(gvl_generator):
    # _prepare_data() stores the month-filtered budget on self.budget_month
    filtered = gvl_generator.budget_month
    months = filtered["Date"].dt.month.unique().tolist()
    assert months == [2], f"Expected only February budget rows, got months: {months}"


def test_gvl_headers_reflect_report_date(gvl_generator):
    headers = gvl_generator.get_report_headers()
    header_str = " ".join(headers)
    assert "Feb-26" in header_str, f"Expected 'Feb-26' in headers, got: {headers}"
    assert "Mar-26" not in header_str, f"'Mar-26' must not appear in headers: {headers}"


# ============================================================
# Step 6 — config.py UTC fix
# ============================================================

def test_report_date_str_with_reference():
    """March 2 2026 (Monday) → last working day is Friday 2026-02-27."""
    from dispatch_reports.config import report_date_str
    result = report_date_str(reference_date=datetime.datetime(2026, 3, 2))
    assert result == "27.02.2026", f"Got: {result}"


def test_report_date_str_no_reference_uses_utcnow():
    """Without reference, report_date_str() still returns a non-empty date string."""
    from dispatch_reports.config import report_date_str
    result = report_date_str()
    assert len(result) == 10 and result[2] == "." and result[5] == "."


def test_report_mtd_banner_with_reference():
    from dispatch_reports.config import report_mtd_banner
    banner = report_mtd_banner(reference_date=datetime.datetime(2026, 3, 2))
    assert "February" in banner, f"Got: {banner}"
    assert "2026" in banner


# ============================================================
# Step 7 — derive_report_date()
# ============================================================

def test_extract_date_priority_over_load_timestamp(tmp_path):
    from dispatch_reports.report_collector import derive_report_date

    # Write a unified CSV with both columns; Extract_Date says Feb, Load_Timestamp says Mar
    csv_path = tmp_path / "qry_unified_mapped_2026.csv"
    pd.DataFrame({
        "Extract_Date": ["2026-02-27"],
        "Load_Timestamp": ["2026-03-02"],
        "value": [1.0],
    }).to_csv(csv_path, index=False)

    report_date = derive_report_date(tmp_path)
    assert report_date.month == 2, (
        f"Expected month 2 (from Extract_Date), got {report_date.month} (from Load_Timestamp)"
    )


def test_load_timestamp_fallback(tmp_path, caplog):
    from dispatch_reports.report_collector import derive_report_date

    csv_path = tmp_path / "qry_unified_mapped_2026.csv"
    pd.DataFrame({
        "Load_Timestamp": ["2026-02-27"],
        "value": [1.0],
    }).to_csv(csv_path, index=False)

    with caplog.at_level(logging.WARNING):
        report_date = derive_report_date(tmp_path)

    assert report_date.month == 2
    assert any("Load_Timestamp" in r.message for r in caplog.records), (
        "Expected a WARNING mentioning Load_Timestamp fallback"
    )


def test_derive_report_date_no_csv_falls_back(tmp_path):
    from dispatch_reports.report_collector import derive_report_date

    # Empty directory — no CSV at all
    report_date = derive_report_date(tmp_path)
    # Just verify it returns a datetime without crashing
    assert isinstance(report_date, datetime.datetime)


# ============================================================
# Step 0B — qry_data_ingestion header-aware parsing
# ============================================================

def _write_qry_file(tmp_path: Path, content: str, name: str = "QRY_AR_MTD_Gmbh.csv") -> str:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return str(p)


def test_ingestion_header_row_strips_date_column(tmp_path):
    from qry_data_ingestion import parse_qry_file_batch

    content = (
        "SlpName=Extract_Date_Int=Total AR Invoice=\n"
        "S. Wöhrle=20260302=377,000000=\n"
        "e-commerce=20260302=288,810000=\n"
    )
    path = _write_qry_file(tmp_path, content)
    rows = parse_qry_file_batch([path])

    entities = [r[0] for r in rows]
    assert "S. Wöhrle" in entities, f"Entity key should be 'S. Wöhrle', got: {entities}"
    assert not any("20260302" in e for e in entities), (
        f"Date must not appear in entity keys; got: {entities}"
    )

    extract_dates = [r[6] for r in rows]
    assert all(pd.notna(d) for d in extract_dates), "Extract_Date should be set for all rows"
    assert extract_dates[0] == pd.Timestamp("2026-03-02"), (
        f"Expected Extract_Date == 2026-03-02, got {extract_dates[0]}"
    )


def test_ingestion_multi_column_header_strips_correctly(tmp_path):
    """Files where CardName appears before Extract_Date_Int."""
    from qry_data_ingestion import parse_qry_file_batch

    content = (
        "Customer No=CardName=Extract_Date_Int=Total Credit Notes=\n"
        "C001=Müller GmbH=20260302=12500,000000=\n"
    )
    path = _write_qry_file(tmp_path, content, "QRY_CN_MTD_Export.csv")
    rows = parse_qry_file_batch([path])

    entities = [r[0] for r in rows]
    assert not any("20260302" in e for e in entities), (
        f"Date must not appear in entity key; got: {entities}"
    )
    assert rows[0][6] == pd.Timestamp("2026-03-02")


def test_ingestion_no_header_returns_nat(tmp_path):
    """Old-format files without Extract_Date_Int header produce NaT Extract_Date."""
    from qry_data_ingestion import parse_qry_file_batch

    content = (
        "S. Wöhrle=377,000000=\n"
        "e-commerce=288,810000=\n"
    )
    path = _write_qry_file(tmp_path, content)
    rows = parse_qry_file_batch([path])

    entities = [r[0] for r in rows]
    assert "S. Wöhrle" in entities, f"Entity key wrong: {entities}"

    extract_dates = [r[6] for r in rows]
    assert all(d is None for d in extract_dates), (
        f"Files without header should produce None Extract_Date; got: {extract_dates}"
    )


def test_ingestion_missing_extract_date_col_files_still_parse(tmp_path):
    """Files without Extract_Date_Int (like CH SO files) parse correctly with None date."""
    from qry_data_ingestion import parse_qry_file_batch

    content = (
        "SlpName=Total Open Orders=\n"
        "S. Wöhrle=45,000000=\n"
    )
    path = _write_qry_file(tmp_path, content, "QRY_SO_OPEN_MTD_CH.csv")
    rows = parse_qry_file_batch([path])

    assert len(rows) == 1
    assert rows[0][0] == "S. Wöhrle"
    assert rows[0][1] == 45.0
    assert rows[0][6] is None
