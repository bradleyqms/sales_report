import importlib.util
import json
from pathlib import Path
import sys

import pandas as pd

SCRIPT_PATH = Path(__file__).parent.parent / "src" / "full_report_v2.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
spec = importlib.util.spec_from_file_location("full_report_v2", SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)


class DummyReportGenerator:
    def __init__(self, *args, **kwargs):
        self._calls = []

    def calculate_report(self):
        return pd.DataFrame(
            [
                {
                    "label": "Dummy",
                    "sales": 10.0,
                    "actual": 10.0,
                    "budget": 12.0,
                    "prior": 9.0,
                    "is_spacer": False,
                    "is_total": True,
                    "is_grand_total": False,
                }
            ]
        )

    def render_report(self, df):
        return None

    def export_report(self, df, csv_path):
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)


def _write_minimum_unified_fixture(csv_path: Path):
    sep = "\x1f"
    lines = [
        sep.join(
            [
                "Region",
                "Currency",
                "Extract_Date_Int",
                "Entity_Type",
                "Entity_Code",
                "Entity_Name",
                "Net_Value",
                "Document_Type",
            ]
        ) + sep,
        sep.join(["USA", "USD", "20260310", "customer", "C100", "Retail Partner", "1000,00", "AR"]) + sep,
        sep.join(["GMBH", "EUR", "20260310", "sales employee", "E200", "Alice", "2000,00", "CN"]) + sep,
    ]
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_full_report_v2_mtd_output_naming(monkeypatch, tmp_path):
    source = tmp_path / "new_unified_dbo_qry_mtd.csv"
    mapping = tmp_path / "entity_mappings.csv"
    output_dir = tmp_path / "outputs"

    _write_minimum_unified_fixture(source)
    pd.DataFrame(
        {
            "Sales_Employee": [],
            "Customer_Name": [],
            "Market_Group": [],
            "Region": [],
            "Channel_Level": [],
            "Company_Group": [],
        }
    ).to_csv(mapping, index=False)

    monkeypatch.setattr(module, "ManagementReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "USASpaReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "CoreMarketReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "apply_mappings", lambda df, mapping_df, output_dir=None: df.copy())
    monkeypatch.setattr(
        module,
        "build_combined_dataframe",
        lambda receivables_df, usa_df, core_df: pd.DataFrame(
            [{"label": "Combined", "sales": 1.0, "budget": 1.0, "prior": 1.0}]
        ),
    )

    module.main(
        [
            "--report-type",
            "MTD",
            "--input-unified-csv",
            str(source),
            "--mapping-file",
            str(mapping),
            "--output-dir",
            str(output_dir),
            "--output-tag",
            "itest",
            "--force-period",
            "2026-03-10",
            "--schema-mode",
            "strict",
        ]
    )

    files = [p.name for p in output_dir.rglob("*.csv")]

    assert any(name.startswith("qry_unified_mapped_2026_MTD_20260310_itest_") for name in files)
    assert any(name.startswith("management_report_usa_spa_2026_MTD_20260310_itest_") for name in files)
    assert any(name.startswith("management_report_core_markets_2026_MTD_20260310_itest_") for name in files)
    assert any(name.startswith("combined_management_report_2026_MTD_20260310_itest_") for name in files)


def test_full_report_v2_eom_business_day_output_naming(monkeypatch, tmp_path):
    source = tmp_path / "new_unified_dbo_qry_eom.csv"
    mapping = tmp_path / "entity_mappings.csv"
    output_dir = tmp_path / "outputs_eom"

    sep = "\x1f"
    lines = [
        sep.join(
            [
                "Region",
                "Currency",
                "Extract_Date_Int",
                "Entity_Type",
                "Entity_Code",
                "Entity_Name",
                "Net_Value",
                "Document_Type",
            ]
        ) + sep,
        sep.join(["USA", "USD", "20260227", "customer", "C100", "Retail Partner", "1000,00", "AR"]) + sep,
    ]
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    pd.DataFrame(
        {
            "Sales_Employee": [],
            "Customer_Name": [],
            "Market_Group": [],
            "Region": [],
            "Channel_Level": [],
            "Company_Group": [],
        }
    ).to_csv(mapping, index=False)

    monkeypatch.setattr(module, "ManagementReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "USASpaReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "CoreMarketReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "apply_mappings", lambda df, mapping_df, output_dir=None: df.copy())
    monkeypatch.setattr(
        module,
        "build_combined_dataframe",
        lambda receivables_df, usa_df, core_df: pd.DataFrame(
            [{"label": "Combined", "sales": 1.0, "budget": 1.0, "prior": 1.0}]
        ),
    )

    module.main(
        [
            "--report-type",
            "EOM",
            "--input-unified-csv",
            str(source),
            "--mapping-file",
            str(mapping),
            "--output-dir",
            str(output_dir),
            "--output-tag",
            "itest",
            "--force-period",
            "2026-02-28",
            "--schema-mode",
            "strict",
            "--eom-completeness-policy",
            "business-day",
        ]
    )

    files = [p.name for p in output_dir.rglob("*.csv")]

    assert any(name.startswith("qry_unified_mapped_2026_EOM_20260228_itest_") for name in files)
    assert any(name.startswith("combined_management_report_2026_EOM_20260228_itest_") for name in files)


def test_full_report_v2_dry_run_does_not_write_outputs(monkeypatch, tmp_path):
    source = tmp_path / "new_unified_dbo_qry_mtd.csv"
    output_dir = tmp_path / "outputs_dry"

    _write_minimum_unified_fixture(source)

    # Should never be called in dry-run path.
    monkeypatch.setattr(module, "apply_mappings", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("apply_mappings called in dry-run")))

    module.main(
        [
            "--report-type",
            "MTD",
            "--input-unified-csv",
            str(source),
            "--output-dir",
            str(output_dir),
            "--output-tag",
            "itest",
            "--force-period",
            "2026-03-10",
            "--schema-mode",
            "strict",
            "--dry-run",
        ]
    )

    assert list(output_dir.rglob("*.csv")) == []
    assert list(output_dir.rglob("*.json")) == []


def test_full_report_v2_writes_run_summary(monkeypatch, tmp_path):
    source = tmp_path / "new_unified_dbo_qry_mtd.csv"
    mapping = tmp_path / "entity_mappings.csv"
    output_dir = tmp_path / "outputs_summary"

    _write_minimum_unified_fixture(source)
    pd.DataFrame(
        {
            "Sales_Employee": [],
            "Customer_Name": [],
            "Market_Group": [],
            "Region": [],
            "Channel_Level": [],
            "Company_Group": [],
        }
    ).to_csv(mapping, index=False)

    monkeypatch.setattr(module, "ManagementReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "USASpaReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "CoreMarketReportGenerator", DummyReportGenerator)
    monkeypatch.setattr(module, "apply_mappings", lambda df, mapping_df, output_dir=None: df.copy())
    monkeypatch.setattr(
        module,
        "build_combined_dataframe",
        lambda receivables_df, usa_df, core_df: pd.DataFrame(
            [{"label": "Combined", "sales": 1.0, "budget": 1.0, "prior": 1.0}]
        ),
    )

    module.main(
        [
            "--report-type",
            "MTD",
            "--input-unified-csv",
            str(source),
            "--mapping-file",
            str(mapping),
            "--output-dir",
            str(output_dir),
            "--output-tag",
            "itest",
            "--force-period",
            "2026-03-10",
            "--schema-mode",
            "strict",
        ]
    )

    summaries = list(output_dir.rglob("v2_run_*_summary.json"))
    assert len(summaries) == 1

    with open(summaries[0], "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    assert payload.get("status") == "success"
    assert payload.get("counts", {}).get("rows_in") == 2


def test_full_report_v2_eom_realistic_fixture_strict_dry_run_passes(tmp_path):
    fixture = tmp_path / "unified_eom_realistic_sanitized.csv"
    output_dir = tmp_path / "outputs_eom_fixture"

    sep = "\x1f"
    fixture.write_text(
        "\n".join(
            [
                sep.join(
                    [
                        "Region",
                        "Currency",
                        "Extract_Date_Int",
                        "Entity_Type",
                        "Entity_Code",
                        "Entity_Name",
                        "Net_Value",
                        "Document_Type",
                    ]
                ) + sep,
                sep.join(["UK", "GBP", "20260227", "Customer", "51189", "RTL Limited", "2461,750000", "AR"]) + sep,
                sep.join(["USA", "USD", "20260227", "Customer", "25032", "Four Seasons Las Vegas", "5114,090000", "AR"]) + sep,
                sep.join(["GmbH", "EUR", "20260227", "Sales_Employee", "", "Interco", "2000,000000", "CN"]) + sep,
            ]
        ) + "\n",
        encoding="utf-8",
    )

    module.main(
        [
            "--report-type",
            "EOM",
            "--input-unified-csv",
            str(fixture),
            "--output-dir",
            str(output_dir),
            "--output-tag",
            "itest",
            "--force-period",
            "2026-02-28",
            "--schema-mode",
            "strict",
            "--eom-completeness-policy",
            "business-day",
            "--dry-run",
        ]
    )

    assert list(output_dir.rglob("*.csv")) == []
