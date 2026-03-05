import datetime
import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest

SCRIPT_PATH = Path(__file__).parent.parent / 'src' / 'eom_workbook_report.py'
sys.path.insert(0, str(SCRIPT_PATH.parent))
spec = importlib.util.spec_from_file_location('eom_workbook_report', SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

parse_force_period = module.parse_force_period
load_regional_workbook = module.load_regional_workbook


def test_parse_force_period_month_string_returns_eom():
    dt = parse_force_period('2026-02')
    assert dt == datetime.datetime(2026, 2, 28)


def test_parse_force_period_date_string_returns_same_month_eom():
    dt = parse_force_period('2026-02-03')
    assert dt == datetime.datetime(2026, 2, 28)


def test_load_regional_workbook_normalizes_required_columns(tmp_path):
    workbook = tmp_path / 'regional.xlsx'

    gmbh = pd.DataFrame({
        'Sales Employee': ['Alice'],
        'CardName': ['Customer A'],
        'Net Value': [1000],
        'Extract_Date_Int': [20260228],
    })
    uk = pd.DataFrame({
        'Sales Employee': ['Bob'],
        'CardName': ['Customer B'],
        'Amount': [2000],
    })
    ag = pd.DataFrame({
        'Sales Employee': ['Carla'],
        'CardName': ['Customer C'],
        'Value': [3000],
    })
    inc = pd.DataFrame({
        'Sales Employee': ['Dan'],
        'CardName': ['Customer D'],
        'Total Value': [4000],
    })

    with pd.ExcelWriter(workbook) as writer:
        gmbh.to_excel(writer, sheet_name='gmbh', index=False)
        uk.to_excel(writer, sheet_name='uk', index=False)
        ag.to_excel(writer, sheet_name='ag', index=False)
        inc.to_excel(writer, sheet_name='inc', index=False)

    report_date = datetime.datetime(2026, 2, 28)
    result = load_regional_workbook(str(workbook), report_date=report_date, strict_sheets=True)

    assert len(result) == 4
    assert set(['Sales Employee Name', 'Customer Name', 'Total Value (EUR)', 'Document Type', 'Company Entity']).issubset(result.columns)
    assert set(result['Company Entity'].unique()) == {'GmbH', 'UK', 'AG', 'USA'}
    assert (result['Extract_Date'] == pd.Timestamp('2026-02-28')).all()


def test_load_regional_workbook_strict_missing_sheet_raises(tmp_path):
    workbook = tmp_path / 'regional_missing.xlsx'
    gmbh = pd.DataFrame({'Net Value': [1000]})

    with pd.ExcelWriter(workbook) as writer:
        gmbh.to_excel(writer, sheet_name='gmbh', index=False)

    with pytest.raises(ValueError, match='Missing required sheets'):
        load_regional_workbook(str(workbook), strict_sheets=True)


def test_load_regional_workbook_includes_optional_export_sheet(tmp_path):
    workbook = tmp_path / 'regional_with_export.xlsx'

    gmbh = pd.DataFrame({'Net Value': [1000]})
    ag = pd.DataFrame({'Net Value': [1100]})
    uk = pd.DataFrame({'Net Value': [1200]})
    inc = pd.DataFrame({'Net Value': [1300]})
    export = pd.DataFrame({'Net Value': [1400], 'CardName': ['Export Customer']})

    with pd.ExcelWriter(workbook) as writer:
        gmbh.to_excel(writer, sheet_name='gmbh', index=False)
        ag.to_excel(writer, sheet_name='ag', index=False)
        uk.to_excel(writer, sheet_name='uk', index=False)
        inc.to_excel(writer, sheet_name='inc', index=False)
        export.to_excel(writer, sheet_name='export', index=False)

    result = load_regional_workbook(str(workbook), strict_sheets=True)
    assert 'Export' in set(result['Company Entity'].unique())
