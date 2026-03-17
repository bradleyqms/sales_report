import datetime
import importlib.util
from pathlib import Path
import sys

import pandas as pd

SCRIPT_PATH = Path(__file__).parent.parent / "src" / "v2_unified_qry_ingestion.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
spec = importlib.util.spec_from_file_location("v2_unified_qry_ingestion", SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

load_unified_qry_csv = module.load_unified_qry_csv
get_schema_manifest_version = module.get_schema_manifest_version


def test_unified_ingestion_maps_required_columns(tmp_path):
    source = tmp_path / "new_unified_dbo_qry_mtd.csv"
    df = pd.DataFrame(
        {
            "Region": ["USA", "GMBH"],
            "Entity_Type": ["customer", "sales employee"],
            "Entity_Name": ["Retail Partner", "Alice"],
            "Net_Value": [1000, 2000],
            "Currency": ["USD", "EUR"],
            "Extract_Date_Int": [20260310, 20260310],
            "Entity_Code": ["C100", "E200"],
            "Document_Type": ["AR", "CN"],
        }
    )
    df.to_csv(source, index=False)

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


def test_unified_ingestion_realistic_equals_delimited_fixture_strict_mode():
    fixture = Path(__file__).parent / "fixtures" / "unified_mtd_realistic_sanitized.csv"

    result = load_unified_qry_csv(
        str(fixture),
        report_type="MTD",
        report_date=datetime.datetime(2026, 3, 10),
        schema_mode="strict",
    )

    assert len(result) == 4
    assert set(result["Currency"].unique()) == {"CHF", "EUR", "USD"}
    assert (result.loc[result["Document Type"] == "CN", "Total Value (EUR)"] < 0).all()


def test_schema_manifest_version_is_loaded_from_default_manifest():
    version = get_schema_manifest_version()
    assert version == "v1"
