import datetime
import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest

SCRIPT_PATH = Path(__file__).parent.parent / "src" / "eom_unified_workbook_report.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
spec = importlib.util.spec_from_file_location("eom_unified_workbook_report", SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

parse_force_period = module.parse_force_period
load_workbook = module.load_workbook


def test_parse_force_period_month_string_returns_eom():
    dt = parse_force_period("2026-02")
    assert dt == datetime.datetime(2026, 2, 28)


def test_parse_force_period_date_string_returns_same_month_eom():
    dt = parse_force_period("2026-02-03")
    assert dt == datetime.datetime(2026, 2, 28)


def test_load_workbook_unified_format_in_strict_mode(tmp_path):
    workbook = tmp_path / "unified.xlsx"
    unified = pd.DataFrame(
        {
            "Region": ["USA", "GMBH"],
            "Entity_Type": ["customer", "sales employee"],
            "Entity_Name": ["Retail Partner", "Alice"],
            "Net_Value": [1000, 2000],
            "Currency": ["USD", "EUR"],
        }
    )

    with pd.ExcelWriter(workbook) as writer:
        unified.to_excel(writer, sheet_name="Sheet1", index=False)

    report_date = datetime.datetime(2026, 2, 28)
    result = load_workbook(workbook, report_date=report_date, strict_sheets=True)

    assert len(result) == 2
    assert set(["Company Entity", "Total Value (EUR)", "Extract_Date"]).issubset(result.columns)
    assert set(result["Company Entity"].unique()) == {"USA", "GmbH"}
    assert (result["Extract_Date"] == pd.Timestamp("2026-02-28")).all()


def test_load_workbook_strict_missing_required_structure_raises(tmp_path):
    workbook = tmp_path / "bad_unified.xlsx"
    bad = pd.DataFrame({"Foo": [1], "Bar": [2]})

    with pd.ExcelWriter(workbook) as writer:
        bad.to_excel(writer, sheet_name="Sheet1", index=False)

    with pytest.raises(ValueError):
        load_workbook(workbook, report_date=datetime.datetime(2026, 2, 28), strict_sheets=True)
