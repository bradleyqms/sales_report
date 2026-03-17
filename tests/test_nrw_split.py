"""Tests for NRW Marina/Ulrike split in CoreMarketReportGenerator.

Verifies that:
- core_market_report_structure.json has two NRW rows (no combined 'NRW' row)
- calculate_report() produces separate Marina and Ulrike rows with correct values
- Germany section total and Total Core Markets both aggregate the split rows correctly
"""

import json
import datetime
import textwrap
from pathlib import Path

import pytest
import pandas as pd
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core_market_report import CoreMarketReportGenerator

CONFIG_PATH = Path(__file__).parent.parent / "src" / "config" / "core_market_report_structure.json"
REPORT_DATE = datetime.datetime(2026, 3, 15)

# ---------------------------------------------------------------------------
# Minimal fixtures
# ---------------------------------------------------------------------------

_MINIMAL_CONFIG = {
    "sections": [
        {
            "title": "Germany",
            "show_total": True,
            "items": [
                {"label": "NRW - Marina", "filter_value": "NRW - Marina"},
                {"label": "NRW - Ulrike", "filter_value": "NRW - Ulrike"},
            ],
        },
        {
            "title": "Total Core Markets",
            "is_total": True,
            "components": ["Germany"],
        },
    ]
}

# Sales already carry the mapped Sub Region values produced by apply_mappings().
# The "M. Pfauch Neukd" row exercises the existing/new split; the plain row is existing.
_SALES_CSV = """\
Company Entity,Document Type,Sub Region,Value_in_EUR_converted,Sales Employee Name
GmbH,AR,NRW - Marina,10000,M. Pfauch
GmbH,AR,NRW - Marina,5000,M. Pfauch Neukd
GmbH,AR,NRW - Ulrike,8000,U. Bensmann
"""

# Budget uses 'Sub Region' column matching the canonical keys.
_BUDGET_CSV = """\
Year,Month,Date,Market_Group,Region,Channel_Level,Subchannel / Partner,Sub Region,Sales Employee / Account,Company_Group,Currency,Metric,Value_kEUR,Value_EUR,Existing_Budget_EUR,New_Budget_EUR
2026,3,01/03/2026,Core Markets,Germany,,,NRW - Marina,Marina,Company 1,EUR,Budget,15,15000,13000,2000
2026,3,01/03/2026,Core Markets,Germany,,,NRW - Ulrike,Ulrike,Company 1,EUR,Budget,10,10000,9000,1000
"""

# Prior uses explicit 'Sub Region' column so no entity_mappings lookup is needed.
# One empty field for Channel_Level (single comma) keeps column count aligned with header.
_PRIOR_CSV = """\
Year,Month,Date,Market_Group,Region,Channel_Level,Sub Region,Value_kEUR,Value_EUR
2025,3,01/03/2025,Core Markets,Germany,,NRW - Marina,12,12000
2025,3,01/03/2025,Core Markets,Germany,,NRW - Ulrike,9,9000
"""


@pytest.fixture()
def generator(tmp_path):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps(_MINIMAL_CONFIG))

    sales_f = tmp_path / "sales.csv"
    budget_f = tmp_path / "budget.csv"
    prior_f = tmp_path / "prior.csv"

    for f, content in [(sales_f, _SALES_CSV), (budget_f, _BUDGET_CSV), (prior_f, _PRIOR_CSV)]:
        f.write_text(textwrap.dedent(content).strip())

    return CoreMarketReportGenerator(
        str(cfg),
        str(sales_f),
        str(budget_f),
        str(prior_f),
        report_date=REPORT_DATE,
    )


@pytest.fixture()
def report_df(generator):
    return pd.DataFrame(generator.calculate_report())


# ---------------------------------------------------------------------------
# Config structure tests (read the real on-disk JSON)
# ---------------------------------------------------------------------------


def test_config_has_no_combined_nrw_row():
    """The Germany section must not have a single combined 'NRW' item."""
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    germany = next(s for s in config["sections"] if s.get("title") == "Germany")
    filter_values = [item["filter_value"] for item in germany["items"]]
    assert "NRW" not in filter_values, "Combined 'NRW' row must be removed from config"


def test_config_has_both_nrw_split_rows():
    """The Germany section must contain both 'NRW - Marina' and 'NRW - Ulrike'."""
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    germany = next(s for s in config["sections"] if s.get("title") == "Germany")
    filter_values = [item["filter_value"] for item in germany["items"]]
    assert "NRW - Marina" in filter_values
    assert "NRW - Ulrike" in filter_values


# ---------------------------------------------------------------------------
# Report row presence
# ---------------------------------------------------------------------------


def test_report_has_marina_and_ulrike_rows(report_df):
    """calculate_report() must produce separate Marina and Ulrike rows."""
    labels = report_df["label"].tolist()
    assert "NRW - Marina" in labels
    assert "NRW - Ulrike" in labels


def test_report_has_no_combined_nrw_row(report_df):
    """calculate_report() must not produce a combined 'NRW' row."""
    assert "NRW" not in report_df["label"].tolist()


# ---------------------------------------------------------------------------
# Sales values
# ---------------------------------------------------------------------------


def test_marina_sales_value(report_df):
    """NRW - Marina sales must reflect all AR rows for that sub-region."""
    row = report_df[report_df["label"] == "NRW - Marina"].iloc[0]
    assert abs(row["sales"] - 15.0) < 0.001  # (10000 + 5000) / 1000


def test_ulrike_sales_value(report_df):
    """NRW - Ulrike sales must reflect all AR rows for that sub-region."""
    row = report_df[report_df["label"] == "NRW - Ulrike"].iloc[0]
    assert abs(row["sales"] - 8.0) < 0.001  # 8000 / 1000


# ---------------------------------------------------------------------------
# Budget and prior year
# ---------------------------------------------------------------------------


def test_nrw_budget_values(report_df):
    """Budget lookup must resolve correctly for each NRW sub-region."""
    marina = report_df[report_df["label"] == "NRW - Marina"].iloc[0]
    ulrike = report_df[report_df["label"] == "NRW - Ulrike"].iloc[0]
    assert abs(marina["budget"] - 15.0) < 0.001
    assert abs(ulrike["budget"] - 10.0) < 0.001


def test_nrw_prior_values(report_df):
    """Prior year lookup must resolve correctly for each NRW sub-region."""
    marina = report_df[report_df["label"] == "NRW - Marina"].iloc[0]
    ulrike = report_df[report_df["label"] == "NRW - Ulrike"].iloc[0]
    assert abs(marina["prior"] - 12.0) < 0.001
    assert abs(ulrike["prior"] - 9.0) < 0.001


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def test_germany_total_sums_both_nrw(report_df):
    """Germany section total must include both NRW sub-regions."""
    total = report_df[(report_df["label"] == "Germany") & (report_df["is_total"] == True)].iloc[0]
    assert abs(total["sales"] - 23.0) < 0.001  # (10+5+8) kEUR


def test_total_core_markets_includes_nrw(report_df):
    """Total Core Markets must aggregate both NRW sub-regions."""
    total = report_df[report_df["label"] == "Total Core Markets"].iloc[0]
    assert abs(total["sales"] - 23.0) < 0.001
