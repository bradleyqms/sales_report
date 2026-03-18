"""Tests for dispatch_reports.html_builder — process_report_table and build_html_body."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# The dispatch_reports package uses relative imports, so we import the module
# directly rather than going through the package __init__.
# ---------------------------------------------------------------------------
import importlib.util, sys

_HERE = Path(__file__).parent
_PKG = _HERE.parent / ".python_packages" / "lib" / "site-packages"
if _PKG.exists() and str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

# Load html_builder as a standalone module (avoids needing the full package)
_spec = importlib.util.spec_from_file_location(
    "html_builder",
    _HERE.parent / "dispatch_reports" / "html_builder.py",
)
_mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)                  # type: ignore[union-attr]

process_report_table = _mod.process_report_table
build_html_body = _mod.build_html_body

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_HEADER_ROW = (
    '<tr><th class="left">Market</th>'
    "<th>Actual kEUR</th>"
    "<th>Budget kEUR</th>"
    "<th>LY kEUR</th>"
    "<th>% vs Bdg</th></tr>"
)

_DATA_ROW = (
    '<tr class="data-row">'
    "<td>Germany</td><td>100</td><td>90</td><td>85</td><td>11%</td></tr>"
)

_SUBTOTAL_ROW = (
    '<tr class="total">'
    "<td>Europe Total</td><td>200</td><td>180</td><td>170</td><td>11%</td></tr>"
)

_TOTAL_SALES_ROW = (
    '<tr class="data-row">'
    "<td>Total Sales</td><td>500</td><td>450</td><td>420</td><td>11%</td></tr>"
)

_USA_ROW = (
    '<tr class="data-row">'
    "<td>Northeast</td><td>50</td><td>45</td><td>40</td><td>11%</td></tr>"
)

_BLANK_ROW = "<tr><td></td><td>-</td><td></td><td></td><td></td></tr>"


def _make_html(rows: str, title: str = "QRY Management Report") -> str:
    return (
        f"<h2>{title}</h2>"
        f"<table>{_HEADER_ROW}{rows}</table>"
    )


# ---------------------------------------------------------------------------
# process_report_table tests
# ---------------------------------------------------------------------------

class TestProcessReportTable:
    def test_title_extracted(self):
        html = _make_html(_DATA_ROW, title="My Report")
        title, _, _, _ = process_report_table(html)
        assert title == "My Report"

    def test_no_split_when_no_total_sales(self):
        html = _make_html(_DATA_ROW + _SUBTOTAL_ROW)
        _, _, tables, _ = process_report_table(html)
        assert len(tables) == 1

    def test_splits_at_total_sales(self):
        rows = _DATA_ROW + _TOTAL_SALES_ROW + _USA_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        assert len(tables) == 2

    def test_main_table_includes_total_sales_row(self):
        rows = _DATA_ROW + _TOTAL_SALES_ROW + _USA_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        assert "Total Sales" in tables[0]

    def test_usa_table_does_not_contain_germany(self):
        rows = _DATA_ROW + _TOTAL_SALES_ROW + _USA_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        assert "Germany" not in tables[1]
        assert "Northeast" in tables[1]

    def test_usa_table_repeats_header(self):
        rows = _DATA_ROW + _TOTAL_SALES_ROW + _USA_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        # Header cells should appear in the USA block
        assert "Actual kEUR" in tables[1]

    def test_blank_spacer_rows_stripped(self):
        rows = _DATA_ROW + _BLANK_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        # Blank rows should not become <tr> entries
        blank_trs = re.findall(r"<tr>[^<]*<td[^>]*>\s*</td>", tables[0])
        assert blank_trs == []

    def test_total_sales_row_is_bold(self):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        # The Total Sales <td> should have font-weight:bold in its style
        td_matches = re.findall(
            r'<td style="[^"]*font-weight:bold[^"]*">Total Sales</td>', tables[0]
        )
        assert td_matches, "Total Sales cell must have font-weight:bold"

    def test_total_sales_row_has_top_border(self):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        assert "border-top:2px solid #2c5282" in tables[0]

    def test_summary_html_contains_total_value(self):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        _, summary, _, _ = process_report_table(html)
        assert "500" in summary  # Actual kEUR column

    def test_no_table_returns_raw_html(self):
        html = "<h2>Title</h2><p>No table here</p>"
        title, _, tables, _ = process_report_table(html)
        assert title == "Title"
        assert len(tables) == 1
        assert "No table here" in tables[0]

    def test_subtotal_rows_have_blue_background(self):
        rows = _DATA_ROW + _SUBTOTAL_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        _, _, tables, _ = process_report_table(html)
        # Europe Total is class="total" -> should have blue background
        assert "background:#d0e4ff" in tables[0]

    # --- New behaviour tests ---

    def test_qry_prefix_stripped_from_title(self):
        html = _make_html(_DATA_ROW, title="QRY Management Report")
        title, _, _, _ = process_report_table(html)
        assert title == "Management Report"
        assert not title.startswith("QRY")

    def test_qry_prefix_with_extra_spaces_stripped(self):
        html = _make_html(_DATA_ROW, title="QRY  Core Markets")
        title, _, _, _ = process_report_table(html)
        assert title == "Core Markets"

    def test_no_qry_prefix_title_unchanged(self):
        html = _make_html(_DATA_ROW, title="Global Sales Report")
        title, _, _, _ = process_report_table(html)
        assert title == "Global Sales Report"

    def test_currency_defaults_to_keur(self):
        html = _make_html(_DATA_ROW)
        _, _, _, currency = process_report_table(html)
        assert currency == "kEUR"

    def test_currency_detected_as_kusd(self):
        # Inject 'kUSD' into the HTML (as the report generator would)
        html = _make_html(_DATA_ROW).replace("kEUR", "kUSD")
        _, _, _, currency = process_report_table(html)
        assert currency == "kUSD"

    def test_keur_replaced_with_kusd_in_tables(self):
        # When the HTML contains kUSD, all kEUR labels should be replaced
        html = _make_html(_DATA_ROW).replace("kEUR", "kUSD")
        _, _, tables, _ = process_report_table(html)
        assert "kEUR" not in tables[0]
        assert "kUSD" in tables[0]

    def test_keur_preserved_in_eur_report(self):
        html = _make_html(_DATA_ROW)  # no kUSD in HTML
        _, _, tables, _ = process_report_table(html)
        assert "kEUR" in tables[0]

    def test_keur_replaced_with_kusd_in_title(self):
        # Title like "USA Spa — Regional Breakdown (kEUR)" must become kUSD
        # when the report body contains kUSD values.
        html = _make_html(_DATA_ROW, title="USA Spa — Regional Breakdown (kEUR)").replace(
            "kEUR", "kUSD"  # simulate report generator writing kUSD into the HTML body
        )
        # The title itself was replaced by `.replace("kEUR","kUSD")` above in the raw HTML,
        # so let's construct the case where only the *body* has kUSD but the title still
        # says kEUR — which is what happens when the generator puts kUSD in column headers
        # but the <h2> tag still says (kEUR).
        raw = (
            '<h2>USA Spa \u2014 Regional Breakdown (kEUR)</h2>'
            f'<table>{_HEADER_ROW.replace("kEUR", "kUSD")}{_DATA_ROW}</table>'
        )
        title, _, _, currency = process_report_table(raw)
        assert currency == "kUSD"
        assert "kEUR" not in title
        assert "kUSD" in title

    def test_fallback_summary_from_total_label(self):
        # A row labelled "Total Core Markets" (not "Total Sales") should still
        # produce a summary banner and NOT split the table.
        total_core_row = (
            '<tr class="data-row">'
            "<td>Total Core Markets</td><td>323</td><td>300</td><td>280</td><td>55%</td></tr>"
        )
        html = _make_html(_DATA_ROW + total_core_row)
        _, summary, tables, _ = process_report_table(html)
        assert "323" in summary
        assert "Total Core Markets" in summary
        assert len(tables) == 1  # no split — just the one table

    def test_no_total_row_gives_empty_summary(self):
        # No row starting with "Total" → empty summary
        html = _make_html(_DATA_ROW + _SUBTOTAL_ROW)  # "Europe Total" doesn't start with Total
        _, summary, _, _ = process_report_table(html)
        assert summary == ""


# ---------------------------------------------------------------------------
# build_html_body tests
# ---------------------------------------------------------------------------

class TestBuildHtmlBody:
    def test_no_files_returns_plain_text(self):
        content_type, body = build_html_body([], "Hello")
        assert content_type == "Text"
        assert body == "Hello"

    def test_with_files_returns_html(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        f = tmp_path / "report.html"
        f.write_text(html, encoding="utf-8")
        content_type, body = build_html_body([f], "Intro")
        assert content_type == "HTML"
        assert "<!DOCTYPE html>" in body

    def test_header_banner_present(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        f = tmp_path / "report.html"
        f.write_text(html, encoding="utf-8")
        _, body = build_html_body([f], "Intro")
        assert "QMS Medicosmetics" in body
        assert "Management Sales Report" in body

    def test_custom_banner_title(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        f = tmp_path / "report.html"
        f.write_text(html, encoding="utf-8")
        _, body = build_html_body([f], "Intro", banner_title="Core Market Sales Report")
        assert "Core Market Sales Report" in body
        assert "Management Sales Report" not in body

    def test_usa_subheading_injected_when_split(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW + _USA_ROW
        html = _make_html(rows)
        f = tmp_path / "combined_management_report.html"
        f.write_text(html, encoding="utf-8")
        _, body = build_html_body([f], "Intro")
        assert "USA Spa" in body

    def test_missing_file_logged_and_skipped(self, tmp_path, caplog):
        missing = tmp_path / "does_not_exist.html"
        import logging
        with caplog.at_level(logging.WARNING):
            content_type, body = build_html_body([missing], "Intro")
        assert content_type == "Text"  # falls back to plain
        assert "Could not read" in caplog.text

    def test_intro_appears_in_body(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        f = tmp_path / "r.html"
        f.write_text(html, encoding="utf-8")
        _, body = build_html_body([f], "MY CUSTOM INTRO")
        assert "MY CUSTOM INTRO" in body

    def test_default_footer_note_csv(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        f = tmp_path / "r.html"
        f.write_text(html, encoding="utf-8")
        _, body = build_html_body([f], "Intro")
        assert "Full CSV data files are attached." in body

    def test_custom_footer_note(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows)
        f = tmp_path / "r.html"
        f.write_text(html, encoding="utf-8")
        _, body = build_html_body([f], "Intro", footer_note="The PDF report is attached.")
        assert "The PDF report is attached." in body
        assert "Full CSV data files are attached." not in body

    def test_section_title_resolver_overrides_embedded_h2(self, tmp_path):
        rows = _DATA_ROW + _TOTAL_SALES_ROW
        html = _make_html(rows, title="USA Spa Regional Report (MTD: March 1-16, 2026)")
        f = tmp_path / "management_report_usa_spa_test.html"
        f.write_text(html, encoding="utf-8")
        _, body = build_html_body(
            [f],
            "Intro",
            section_title_resolver=lambda _path, _title: "USA Spa Sales Report (EOM: February 1-28, 2026)",
            banner_subtitle="End-of-month figures",
        )
        assert "USA Spa Sales Report (EOM: February 1-28, 2026)" in body
        assert "USA Spa Regional Report (MTD: March 1-16, 2026)" not in body
        assert "End-of-month figures" in body
