import datetime
import sys
from pathlib import Path

import pandas as pd


SRC = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC))

from base_report_generator import BaseReportGenerator


class _DummyReportGenerator(BaseReportGenerator):
    def __init__(self):
        self.now = datetime.datetime(2026, 3, 19)

    def calculate_report(self):
        raise NotImplementedError

    def render_report(self, df):
        raise NotImplementedError

    def get_report_headers(self):
        return ["kEUR", "Mar-26A MTD", "Budget", "Prior", "% vs Bud"]

    def get_report_title(self):
        return "QRY Management Report"

    def format_row_for_export(self, row):
        return [
            row["label"],
            str(row["sales"]),
            str(row["budget"]),
            str(row["prior"]),
            row["pct"],
        ]


def test_management_report_component_rows_do_not_inherit_subtotal_styling(tmp_path):
    generator = _DummyReportGenerator()
    df = pd.DataFrame([
        {
            "label": "Total Core Markets",
            "sales": 100,
            "budget": 120,
            "prior": 110,
            "pct": "83.3%",
            "is_total": True,
            "is_grand_total": False,
            "is_spacer": False,
            "should_bold": True,
        },
        {
            "label": "",
            "sales": 0,
            "budget": 0,
            "prior": 0,
            "pct": "",
            "is_total": False,
            "is_grand_total": False,
            "is_spacer": True,
            "should_bold": True,
        },
        {
            "label": "Germany",
            "sales": 20,
            "budget": 30,
            "prior": 25,
            "pct": "66.7%",
            "is_total": False,
            "is_grand_total": False,
            "is_spacer": False,
            "should_bold": False,
        },
    ])

    output_csv = tmp_path / "management_report.csv"
    generator.export_report(df, str(output_csv))

    html = output_csv.with_suffix(".html").read_text(encoding="utf-8")

    assert '<tr class="total"><td style="text-align:left;font-weight:bold;">Total Core Markets</td>' in html
    assert '<tr class=""><td style="text-align:left;">Germany</td>' in html
    assert 'class="grand-total"><td style="text-align:left;">Germany</td>' not in html
    assert 'class="total"><td style="text-align:left;">Germany</td>' not in html