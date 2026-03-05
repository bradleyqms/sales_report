import argparse
import calendar
import datetime
import logging
import os
import re
from pathlib import Path

import pandas as pd

from qry_data_mapping import apply_mappings
from receivables_report_generator import ManagementReportGenerator
from usa_spa_report import USASpaReportGenerator
from core_market_report import CoreMarketReportGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

FX_RATES = {"CHF": 1.08, "USD": 0.96, "GBP": 1.20, "EUR": 1.00}

WORKBOOK_SHEET_DEFAULTS = {
    'gmbh': {'Company Entity': 'GmbH', 'Currency': 'EUR'},
    'ag': {'Company Entity': 'AG', 'Currency': 'CHF'},
    'uk': {'Company Entity': 'UK', 'Currency': 'GBP'},
    'inc': {'Company Entity': 'USA', 'Currency': 'USD'}
}

OPTIONAL_WORKBOOK_SHEET_DEFAULTS = {
    'export': {'Company Entity': 'Export', 'Currency': 'EUR'}
}

WORKBOOK_COLUMN_ALIASES = {
    'Sales Employee Name': ['Sales Employee Name', 'Sales Employee', 'Salesperson', 'SlpName', 'Employee', 'Representative'],
    'Customer Name': ['Customer Name', 'Customer', 'CardName', 'Customer_Name', 'Account Name'],
    'Total Value (EUR)': [
        'Total Value (EUR)',
        'Total Value',
        'Net Value',
        'Amount',
        'Sales Amount',
        'Value',
        'Total AR Invoice',
        'Total Credit Notes',
        'Total Open Orders'
    ],
    'Document Type': ['Document Type', 'Doc Type', 'Type'],
    'Customer Code': ['Customer Code', 'Customer No', 'Customer Number', 'CardCode'],
    'Extract_Date': ['Extract_Date', 'Extract Date', 'Extract_Date_Int', 'Posting Date', 'Date'],
    'Currency': ['Currency', 'Curr', 'ISO Currency']
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description='Generate EOM report pack from regional workbook')
    parser.add_argument('--input-xlsx', required=True, help='Path to workbook with gmbh/uk/ag/inc sheets')
    parser.add_argument('--force-period', required=True, help='Forced period in YYYY-MM or YYYY-MM-DD')
    parser.add_argument('--strict-sheets', action='store_true', help='Fail if any required sheet is missing')
    parser.add_argument('--output-tag', default='', help='Optional extra token in output filenames')
    parser.add_argument('--output-dir', default=None, help='Optional output directory override')
    return parser.parse_args(argv)


def parse_force_period(force_period):
    force_period = force_period.strip()
    if len(force_period) == 7:
        year, month = map(int, force_period.split('-'))
    elif len(force_period) == 10:
        parsed = datetime.datetime.strptime(force_period, '%Y-%m-%d')
        year, month = parsed.year, parsed.month
    else:
        raise ValueError('--force-period must be YYYY-MM or YYYY-MM-DD')

    if month < 1 or month > 12:
        raise ValueError('--force-period month must be between 1 and 12')

    eom_day = calendar.monthrange(year, month)[1]
    return datetime.datetime(year, month, eom_day)


def build_period_token(report_date):
    return f"EOM_{report_date.strftime('%Y%m%d')}"


def compose_output_name(prefix, year, timestamp, period_token=None, output_tag=None):
    parts = [prefix, str(year)]
    if period_token:
        parts.append(period_token)
    if output_tag:
        safe_tag = str(output_tag).strip()
        if safe_tag:
            parts.append(safe_tag)
    parts.append(timestamp)
    return '_'.join(parts)


def _normalize_col_name(name):
    return re.sub(r'[^a-z0-9]+', '', str(name).strip().lower())


def _find_matching_column(df, aliases):
    normalized = {_normalize_col_name(col): col for col in df.columns}
    for alias in aliases:
        alias_key = _normalize_col_name(alias)
        if alias_key in normalized:
            return normalized[alias_key]
    return None


def _coerce_extract_date(series):
    if series is None:
        return pd.NaT

    parsed = pd.to_datetime(series, errors='coerce')
    if parsed.notna().any():
        return parsed.dropna().iloc[0]

    parsed_int = pd.to_datetime(series.astype(str), format='%Y%m%d', errors='coerce')
    if parsed_int.notna().any():
        return parsed_int.dropna().iloc[0]

    return pd.NaT


def load_regional_workbook(workbook_path, report_date=None, strict_sheets=False):
    workbook = Path(workbook_path)
    if not workbook.exists():
        raise FileNotFoundError(f'Workbook not found: {workbook}')
    if workbook.suffix.lower() not in ['.xlsx', '.xlsm', '.xls']:
        raise ValueError(f'Workbook must be an Excel file: {workbook}')

    all_sheets = pd.read_excel(workbook, sheet_name=None)
    sheet_lookup = {name.strip().lower(): (name, df) for name, df in all_sheets.items()}

    missing = [s for s in WORKBOOK_SHEET_DEFAULTS if s not in sheet_lookup]
    if strict_sheets and missing:
        raise ValueError(f"Missing required sheets: {', '.join(missing)}")

    frames = []
    load_timestamp = pd.Timestamp.now()

    all_sheet_defaults = {**WORKBOOK_SHEET_DEFAULTS, **OPTIONAL_WORKBOOK_SHEET_DEFAULTS}

    for sheet_key, defaults in all_sheet_defaults.items():
        if sheet_key not in sheet_lookup:
            if sheet_key in WORKBOOK_SHEET_DEFAULTS:
                logging.warning(f"Workbook sheet '{sheet_key}' not found; skipping")
            continue

        original_name, raw_df = sheet_lookup[sheet_key]
        if raw_df is None or raw_df.empty:
            logging.warning(f"Workbook sheet '{original_name}' is empty; skipping")
            continue

        df = raw_df.dropna(how='all').copy()
        if df.empty:
            continue

        sales_col = _find_matching_column(df, WORKBOOK_COLUMN_ALIASES['Sales Employee Name'])
        customer_col = _find_matching_column(df, WORKBOOK_COLUMN_ALIASES['Customer Name'])
        value_col = _find_matching_column(df, WORKBOOK_COLUMN_ALIASES['Total Value (EUR)'])
        doc_type_col = _find_matching_column(df, WORKBOOK_COLUMN_ALIASES['Document Type'])
        customer_code_col = _find_matching_column(df, WORKBOOK_COLUMN_ALIASES['Customer Code'])
        extract_date_col = _find_matching_column(df, WORKBOOK_COLUMN_ALIASES['Extract_Date'])
        currency_col = _find_matching_column(df, WORKBOOK_COLUMN_ALIASES['Currency'])

        if value_col is None:
            raise ValueError(
                f"Sheet '{original_name}' is missing a value column. "
                f"Expected one of: {WORKBOOK_COLUMN_ALIASES['Total Value (EUR)']}"
            )

        canonical = pd.DataFrame()
        canonical['Sales Employee Name'] = df[sales_col] if sales_col else pd.NA
        canonical['Customer Name'] = df[customer_col] if customer_col else pd.NA
        canonical['Total Value (EUR)'] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)
        canonical['Document Type'] = (
            df[doc_type_col].fillna('AR').astype(str).str.strip().replace('', 'AR')
            if doc_type_col else 'AR'
        )

        if report_date is not None:
            canonical['Extract_Date'] = pd.Timestamp(report_date)
        elif extract_date_col:
            parsed_date = _coerce_extract_date(df[extract_date_col])
            canonical['Extract_Date'] = parsed_date if pd.notna(parsed_date) else pd.NaT
        else:
            canonical['Extract_Date'] = pd.NaT

        if customer_code_col:
            canonical['Customer Code'] = df[customer_code_col]
        else:
            canonical['Customer Code'] = pd.NA

        if currency_col:
            canonical['Currency'] = df[currency_col].fillna(defaults['Currency']).astype(str).str.upper().str.strip()
            canonical['Currency'] = canonical['Currency'].replace('', defaults['Currency'])
        else:
            canonical['Currency'] = defaults['Currency']

        canonical['Company Entity'] = defaults['Company Entity']
        canonical['Source_File'] = f"{workbook.name}:{original_name}"
        canonical['Metric'] = 'Receivables'
        canonical['Load_Timestamp'] = load_timestamp
        canonical['Total Open Value (EUR)'] = canonical['Total Value (EUR)']

        fx_multiplier = canonical['Currency'].map(FX_RATES).fillna(1.0)
        canonical['Value_in_EUR_converted'] = canonical['Total Value (EUR)'] * fx_multiplier

        frames.append(canonical)

    if not frames:
        raise ValueError(f'No usable rows found in workbook: {workbook}')

    return pd.concat(frames, ignore_index=True)


def main(argv=None):
    args = parse_args(argv)
    report_date = parse_force_period(args.force_period)
    period_token = build_period_token(report_date)

    start_time = datetime.datetime.now()
    project_root = Path(__file__).parent.parent
    output_dir = args.output_dir or os.environ.get('REPORT_OUTPUT_DIR', str(project_root / 'data/outputs'))
    os.makedirs(output_dir, exist_ok=True)

    current_year = report_date.year
    prior_year = report_date.year - 1
    timestamp = start_time.strftime('%Y%m%d_%H%M%S')

    print('=' * 80)
    print('EOM WORKBOOK REPORT GENERATION')
    print('=' * 80)
    print(f"Starting at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Period anchor: {report_date.strftime('%Y-%m-%d')} ({period_token})")
    print(f"Workbook input: {args.input_xlsx}")
    print()

    qry_df = load_regional_workbook(
        args.input_xlsx,
        report_date=report_date,
        strict_sheets=args.strict_sheets
    )

    mapping_path = project_root / 'data/inputs/mappings/entity_mappings.csv'
    mapping_df = pd.read_csv(mapping_path)
    mapped_df = apply_mappings(qry_df, mapping_df, output_dir=output_dir)

    mapped_name = compose_output_name(
        'qry_unified_mapped',
        current_year,
        timestamp,
        period_token=period_token,
        output_tag=args.output_tag
    )
    mapped_path = Path(output_dir) / f'{mapped_name}.csv'
    mapped_df.to_csv(mapped_path, index=False)

    budget_path = project_root / f'data/inputs/budget/budget_{current_year}_processed.csv'
    prior_path = project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv'
    gvl_budget_path = project_root / f'data/inputs/budget/budget_GVL_{current_year}.csv'
    gvl_prior_path = project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv'
    usa_spa_budget_path = project_root / f'data/inputs/budget/budget_USA_spa_{current_year}.csv'
    usa_spa_prior_path = project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_usa.csv'

    receivables_gen = ManagementReportGenerator(
        str(project_root / 'src/config/report_structure.json'),
        str(mapped_path),
        str(budget_path),
        str(prior_path),
        report_date=report_date
    )
    receivables_df = receivables_gen.calculate_report()
    receivables_gen.render_report(receivables_df)

    usa_spa_gen = USASpaReportGenerator(
        str(project_root / 'src/config/usa_spa_report_structure.json'),
        str(mapped_path),
        str(usa_spa_budget_path),
        str(usa_spa_prior_path),
        report_date=report_date
    )
    usa_spa_df = usa_spa_gen.calculate_report()
    usa_spa_gen.render_report(usa_spa_df)

    core_market_gen = CoreMarketReportGenerator(
        str(project_root / 'src/config/core_market_report_structure.json'),
        str(mapped_path),
        str(gvl_budget_path),
        str(gvl_prior_path),
        report_date=report_date
    )
    core_market_df = core_market_gen.calculate_report()
    core_market_gen.render_report(core_market_df)

    usa_spa_base = compose_output_name(
        'management_report_usa_spa',
        current_year,
        timestamp,
        period_token=period_token,
        output_tag=args.output_tag
    )
    core_market_base = compose_output_name(
        'management_report_core_markets',
        current_year,
        timestamp,
        period_token=period_token,
        output_tag=args.output_tag
    )
    combined_base = compose_output_name(
        'combined_management_report',
        current_year,
        timestamp,
        period_token=period_token,
        output_tag=args.output_tag
    )

    usa_spa_gen.export_report(usa_spa_df, str(Path(output_dir) / f'{usa_spa_base}.csv'))
    core_market_gen.export_report(core_market_df, str(Path(output_dir) / f'{core_market_base}.csv'))

    separator_receivables = pd.DataFrame([{
        'label': '=== RECEIVABLES MANAGEMENT REPORT ===',
        'sales': 0.0, 'budget': 0.0, 'prior': 0.0,
        'is_spacer': True, 'is_total': False, 'is_grand_total': False
    }])
    separator_usa_spa = pd.DataFrame([{
        'label': '=== USA SPA REGIONAL REPORT ===',
        'sales': 0.0, 'budget': 0.0, 'prior': 0.0,
        'is_spacer': True, 'is_total': False, 'is_grand_total': False
    }])

    combined_parts = [separator_receivables, receivables_df, separator_usa_spa, usa_spa_df.rename(columns={'actual': 'sales'})]
    combined_df = pd.concat(combined_parts, ignore_index=True)
    receivables_gen.export_report(combined_df, str(Path(output_dir) / f'{combined_base}.csv'))

    end_time = datetime.datetime.now()
    print()
    print('=' * 80)
    print('EOM WORKBOOK REPORT COMPLETE')
    print('=' * 80)
    print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {(end_time - start_time).total_seconds():.2f} seconds")
    print(f"Output directory: {output_dir}")
    print('=' * 80)


if __name__ == '__main__':
    main()
