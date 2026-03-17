import datetime
import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest

SCRIPT_PATH = Path(__file__).parent.parent / "src" / "v2_unified_qry_ingestion.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
spec = importlib.util.spec_from_file_location("v2_unified_qry_ingestion", SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

load_unified_qry_csv = module.load_unified_qry_csv
get_schema_manifest_version = module.get_schema_manifest_version


def _write_usv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    sep = "\x1f"
    with open(path, "w", encoding="utf-8", newline="") as handle:
        handle.write(sep.join(header) + sep + "\n")
        for row in rows:
            handle.write(sep.join(row) + sep + "\n")


def test_unified_ingestion_maps_required_columns(tmp_path):
    source = tmp_path / "new_unified_dbo_qry_mtd.csv"
    _write_usv(
        source,
        [
            "Region",
            "Currency",
            "Extract_Date_Int",
            "Entity_Type",
            "Entity_Code",
            "Entity_Name",
            "Net_Value",
            "Document_Type",
        ],
        [
            ["USA", "USD", "20260310", "customer", "C100", "Retail Partner", "1000,00", "AR"],
            ["GMBH", "EUR", "20260310", "sales employee", "E200", "Alice", "2000,00", "CN"],
        ],
    )

    result = load_unified_qry_csv(
        str(source),
        report_type="MTD",
        report_date=datetime.datetime(2026, 3, 10),
    )

    required = {
        "Sales Employee Name",
        "Customer Name",
        "Total Value (EUR)",
        "Document Type",
        "Company Entity",
        "Currency",
        "Source_File",
        "Extract_Date",
        "Value_in_EUR_converted",
    }
    assert required.issubset(result.columns)
    assert len(result) == 2
    assert set(result["Company Entity"].unique()) == {"USA", "GmbH"}
    assert (result.loc[result["Document Type"] == "CN", "Total Value (EUR)"] <= 0).all()


def test_unified_ingestion_realistic_unit_separator_fixture_strict_mode(tmp_path):
    fixture = tmp_path / "unified_mtd_realistic_sanitized.csv"
    _write_usv(
        fixture,
        [
            "Region",
            "Currency",
            "Extract_Date_Int",
            "Entity_Type",
            "Entity_Code",
            "Entity_Name",
            "Net_Value",
            "Document_Type",
        ],
        [
            ["CH", "CHF", "20260310", "Sales_Employee", "", "Ch Rose", "7254,720000", "AR"],
            ["Export", "EUR", "20260310", "Customer", "10058", "Mweya Luxury FZCO Dubai", "29443,690000", "AR"],
            ["GmbH", "EUR", "20260310", "Sales_Employee", "", "Interco", "39893,400000", "CN"],
            ["US", "USD", "20260310", "Customer", "40000", "Shopify", "6499,070000", "AR"],
        ],
    )

    result = load_unified_qry_csv(
        str(fixture),
        report_type="MTD",
        report_date=datetime.datetime(2026, 3, 10),
        schema_mode="strict",
    )

    assert len(result) == 4
    assert set(result["Currency"].unique()) == {"CHF", "EUR", "USD"}
    assert (result.loc[result["Document Type"] == "CN", "Total Value (EUR)"] < 0).all()


def test_unified_ingestion_rejects_non_yyyymmdd_extract_date(tmp_path):
    source = tmp_path / "bad_date.csv"
    _write_usv(
        source,
        [
            "Region",
            "Currency",
            "Extract_Date_Int",
            "Entity_Type",
            "Entity_Code",
            "Entity_Name",
            "Net_Value",
            "Document_Type",
        ],
        [["USA", "USD", "2026-03-10", "customer", "C100", "Retail Partner", "1000,00", "AR"]],
    )

    with pytest.raises(ValueError, match="YYYYMMDD"):
        load_unified_qry_csv(str(source), report_type="MTD", report_date=datetime.datetime(2026, 3, 10))


def test_unified_ingestion_accepts_escaped_unit_separator_literal(tmp_path):
    source = tmp_path / "escaped_usv.csv"
    source.write_text(
        "Region\\x1fCurrency\\x1fExtract_Date_Int\\x1fEntity_Type\\x1fEntity_Code\\x1fEntity_Name\\x1fNet_Value\\x1fDocument_Type\\x1f\n"
        "USA\\x1fUSD\\x1f20260310\\x1fcustomer\\x1fC100\\x1fRetail Partner\\x1f1000,00\\x1fAR\\x1f\n",
        encoding="utf-8",
    )

    result = load_unified_qry_csv(
        str(source),
        report_type="MTD",
        report_date=datetime.datetime(2026, 3, 10),
    )

    assert len(result) == 1
    assert result.iloc[0]["Customer Name"] == "Retail Partner"
    assert result.iloc[0]["Document Type"] == "AR"


def test_schema_manifest_version_is_loaded_from_default_manifest():
    version = get_schema_manifest_version()
    assert version == "v1"
