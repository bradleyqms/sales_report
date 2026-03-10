import argparse
import calendar
import datetime
import inspect
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

REQUIRED_SHEETS = {
    "gmbh": {"Company Entity": "GmbH", "Currency": "EUR"},
    "ag": {"Company Entity": "AG", "Currency": "CHF"},
    "uk": {"Company Entity": "UK", "Currency": "GBP"},
    "inc": {"Company Entity": "USA", "Currency": "USD"},
}

OPTIONAL_SHEETS = {
    "export": {"Company Entity": "Export", "Currency": "EUR"},
}

REGION_TO_ENTITY = {
    "GMBH": "GmbH",
    "GMB": "GmbH",
    "CH": "AG",
    "AG": "AG",
    "UK": "UK",
    "USA": "USA",
    "INC": "USA",
    "EXPORT": "Export",
}

COLUMN_ALIASES = {
    "Sales Employee Name": ["Sales Employee Name", "Sales Employee", "Salesperson", "SlpName", "Employee", "Representative"],
    "Customer Name": ["Customer Name", "Customer", "CardName", "Customer_Name", "Account Name"],
    "Total Value (EUR)": [
        "Total Value (EUR)",
        "Total Value",
        "Net Value",
        "Amount",
        "Sales Amount",
        "Value",
        "Total AR Invoice",
        "Total Credit Notes",
        "Total Open Orders",
    ],
    "Document Type": ["Document Type", "Doc Type", "Type", "Document"],
    "Customer Code": ["Customer Code", "Customer No", "Customer Number", "CardCode"],
    "Extract_Date": ["Extract_Date", "Extract Date", "Extract_Date_Int", "Posting Date", "Date"],
    "Currency": ["Currency", "Curr", "ISO Currency"],
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Generate EOM reports from workbook (V2)")
    parser.add_argument("--input-xlsx", default=None, help="Workbook path. If omitted, latest root .xlsx is used.")
    parser.add_argument("--force-period", default=None, help="Period anchor: YYYY-MM or YYYY-MM-DD")
    parser.add_argument("--strict-sheets", action="store_true", help="Require all required sheets (gmbh/ag/uk/inc)")
    parser.add_argument("--mapping-file", default=None, help="Optional mappings CSV override")
    parser.add_argument("--output-tag", default="v2", help="Extra token for output file names")
    parser.add_argument("--output-dir", default=None, help="Optional output directory")
    return parser.parse_args(argv)


def parse_force_period(force_period):
    if not force_period:
        return None

    value = force_period.strip()
    if len(value) == 7:
        year, month = map(int, value.split("-"))
    elif len(value) == 10:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%d")
        year, month = parsed.year, parsed.month
    else:
        raise ValueError("--force-period must be YYYY-MM or YYYY-MM-DD")

    day = calendar.monthrange(year, month)[1]
    return datetime.datetime(year, month, day)


def build_period_token(report_date):
    return f"EOM_{report_date.strftime('%Y%m%d')}"


def compose_output_name(prefix, year, timestamp, period_token, output_tag):
    parts = [prefix, str(year), period_token]
    if output_tag:
        clean = str(output_tag).strip()
        if clean:
            parts.append(clean)
    parts.append(timestamp)
    return "_".join(parts)


def normalize_col_name(name):
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def find_matching_column(df, aliases):
    normalized = {normalize_col_name(col): col for col in df.columns}
    for alias in aliases:
        key = normalize_col_name(alias)
        if key in normalized:
            return normalized[key]
    return None


def coerce_date(series):
    if series is None:
        return pd.NaT

    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    if parsed.notna().any():
        return parsed.dropna().iloc[0]

    parsed_compact = pd.to_datetime(series.astype(str), format="%Y%m%d", errors="coerce")
    if parsed_compact.notna().any():
        return parsed_compact.dropna().iloc[0]

    return pd.NaT


def resolve_input_xlsx(project_root, explicit_path=None):
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Workbook not found: {path}")
        return path

    candidates = sorted(project_root.glob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError("No .xlsx files found in project root. Pass --input-xlsx.")
    return candidates[0]


def resolve_mapping_file(project_root, explicit_path=None):
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_absolute():
            path = (project_root / path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Mapping file not found: {path}")
        return path

    preferred = [
        project_root / "data/inputs/mappings/entity_mappings.csv",
        project_root / "data/inputs/mappings/py25_regional_mappings.csv",
    ]
    for path in preferred:
        if path.exists():
            return path
    raise FileNotFoundError("No mapping file found in expected locations.")


def apply_report_date_anchor(generator, report_date):
    """Force date-anchor attributes for generator compatibility across versions."""
    generator._report_date = report_date
    generator.now = report_date
    generator.current_year = report_date.year
    generator.prior_year = report_date.year - 1
    generator.current_month = report_date.month


def infer_report_date_from_workbook(workbook_path):
    all_sheets = pd.read_excel(workbook_path, sheet_name=None)
    candidates = []
    for _, df in all_sheets.items():
        if df is None or df.empty:
            continue
        for alias in COLUMN_ALIASES["Extract_Date"]:
            col = find_matching_column(df, [alias])
            if col:
                dt = coerce_date(df[col])
                if pd.notna(dt):
                    candidates.append(dt)
    if not candidates:
        now = datetime.datetime.now()
        return datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1])
    latest = max(candidates)
    return datetime.datetime(latest.year, latest.month, calendar.monthrange(latest.year, latest.month)[1])


def load_workbook(workbook_path, report_date, strict_sheets=False):
    all_sheets = pd.read_excel(workbook_path, sheet_name=None)
    lookup = {name.strip().lower(): (name, df) for name, df in all_sheets.items()}

    missing = [key for key in REQUIRED_SHEETS if key not in lookup]
    if strict_sheets and missing:
        # Allow strict mode for unified one-sheet workbook format as an alternative.
        if len(all_sheets) != 1:
            raise ValueError(f"Missing required sheets: {', '.join(missing)}")
        only_name, only_df = next(iter(all_sheets.items()))
        normalized_cols = {normalize_col_name(c) for c in only_df.columns}
        required_unified = {
            normalize_col_name("Region"),
            normalize_col_name("Entity_Type"),
            normalize_col_name("Entity_Name"),
            normalize_col_name("Net_Value"),
        }
        if not required_unified.issubset(normalized_cols):
            raise ValueError(f"Missing required sheets: {', '.join(missing)}")
        return load_unified_workbook(all_sheets, workbook_path, report_date)

    sheet_defaults = {**REQUIRED_SHEETS, **OPTIONAL_SHEETS}
    frames = []
    load_ts = pd.Timestamp.now()

    for sheet_key, defaults in sheet_defaults.items():
        if sheet_key not in lookup:
            if sheet_key in REQUIRED_SHEETS:
                logging.warning("Required sheet '%s' not found; skipping", sheet_key)
            continue

        sheet_name, raw_df = lookup[sheet_key]
        if raw_df is None or raw_df.empty:
            logging.warning("Sheet '%s' is empty; skipping", sheet_name)
            continue

        df = raw_df.dropna(how="all").copy()
        if df.empty:
            continue

        sales_col = find_matching_column(df, COLUMN_ALIASES["Sales Employee Name"])
        customer_col = find_matching_column(df, COLUMN_ALIASES["Customer Name"])
        value_col = find_matching_column(df, COLUMN_ALIASES["Total Value (EUR)"])
        doc_col = find_matching_column(df, COLUMN_ALIASES["Document Type"])
        customer_code_col = find_matching_column(df, COLUMN_ALIASES["Customer Code"])
        currency_col = find_matching_column(df, COLUMN_ALIASES["Currency"])

        if not value_col:
            raise ValueError(
                f"Sheet '{sheet_name}' has no supported value column. Expected one of {COLUMN_ALIASES['Total Value (EUR)']}"
            )

        canonical = pd.DataFrame()
        canonical["Sales Employee Name"] = df[sales_col] if sales_col else pd.NA
        canonical["Customer Name"] = df[customer_col] if customer_col else pd.NA
        canonical["Total Value (EUR)"] = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
        canonical["Document Type"] = (
            df[doc_col].fillna("AR").astype(str).str.strip().replace("", "AR")
            if doc_col else "AR"
        )
        canonical["Extract_Date"] = pd.Timestamp(report_date)
        canonical["Customer Code"] = df[customer_code_col] if customer_code_col else pd.NA

        if currency_col:
            canonical["Currency"] = df[currency_col].fillna(defaults["Currency"]).astype(str).str.upper().str.strip()
            canonical["Currency"] = canonical["Currency"].replace("", defaults["Currency"])
        else:
            canonical["Currency"] = defaults["Currency"]

        canonical["Company Entity"] = defaults["Company Entity"]
        canonical["Source_File"] = f"{workbook_path.name}:{sheet_name}"
        canonical["Metric"] = "Receivables"
        canonical["Load_Timestamp"] = load_ts
        canonical["Total Open Value (EUR)"] = canonical["Total Value (EUR)"]

        fx = canonical["Currency"].map(FX_RATES).fillna(1.0)
        canonical["Value_in_EUR_converted"] = canonical["Total Value (EUR)"] * fx

        frames.append(canonical)

    if not frames:
        raise ValueError(f"No usable rows found in workbook: {workbook_path}")

    return pd.concat(frames, ignore_index=True)


def load_unified_workbook(all_sheets, workbook_path, report_date):
    sheet_name, df = next(iter(all_sheets.items()))
    if df is None or df.empty:
        raise ValueError(f"Unified workbook sheet '{sheet_name}' is empty")

    region_col = find_matching_column(df, ["Region"]) 
    currency_col = find_matching_column(df, ["Currency"]) 
    entity_type_col = find_matching_column(df, ["Entity_Type", "Entity Type"]) 
    entity_code_col = find_matching_column(df, ["Entity_Code", "Entity Code", "Customer Code"]) 
    entity_name_col = find_matching_column(df, ["Entity_Name", "Entity Name", "Customer Name", "Sales Employee Name"]) 
    value_col = find_matching_column(df, ["Net_Value", "Net Value", "Total Value", "Amount"]) 

    if not all([region_col, entity_type_col, entity_name_col, value_col]):
        raise ValueError(
            "Unified workbook is missing required columns. "
            "Expected at least Region, Entity_Type, Entity_Name, Net_Value"
        )

    out = pd.DataFrame()
    out["Total Value (EUR)"] = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
    out["Document Type"] = "AR"
    out["Extract_Date"] = pd.Timestamp(report_date)

    if currency_col:
        out["Currency"] = df[currency_col].fillna("EUR").astype(str).str.upper().str.strip()
        out["Currency"] = out["Currency"].replace("", "EUR")
    else:
        out["Currency"] = "EUR"

    region_raw = df[region_col].fillna("").astype(str).str.upper().str.strip()
    out["Company Entity"] = region_raw.map(REGION_TO_ENTITY).fillna(region_raw)

    entity_type = df[entity_type_col].fillna("").astype(str).str.lower().str.strip()
    entity_name = df[entity_name_col]
    is_sales_employee = entity_type.str.contains("sales") | entity_type.str.contains("employee")
    out["Sales Employee Name"] = entity_name.where(is_sales_employee, pd.NA)
    out["Customer Name"] = entity_name.where(~is_sales_employee, pd.NA)

    if entity_code_col:
        out["Customer Code"] = df[entity_code_col]
    else:
        out["Customer Code"] = pd.NA

    out["Source_File"] = f"{workbook_path.name}:{sheet_name}"
    out["Metric"] = "Receivables"
    out["Load_Timestamp"] = pd.Timestamp.now()
    out["Total Open Value (EUR)"] = out["Total Value (EUR)"]

    fx = out["Currency"].map(FX_RATES).fillna(1.0)
    out["Value_in_EUR_converted"] = out["Total Value (EUR)"] * fx
    return out


def main(argv=None):
    args = parse_args(argv)

    project_root = Path(__file__).resolve().parents[1]
    workbook = resolve_input_xlsx(project_root, args.input_xlsx)

    report_date = parse_force_period(args.force_period)
    if report_date is None:
        report_date = infer_report_date_from_workbook(workbook)

    current_year = report_date.year
    prior_year = report_date.year - 1
    period_token = build_period_token(report_date)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    output_dir = Path(args.output_dir) if args.output_dir else Path(os.environ.get("REPORT_OUTPUT_DIR", str(project_root / "data/outputs")))
    if not output_dir.is_absolute():
        output_dir = (project_root / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    mapping_path = resolve_mapping_file(project_root, args.mapping_file)

    print("=" * 80)
    print("EOM WORKBOOK V2 REPORT GENERATION")
    print("=" * 80)
    print(f"Workbook: {workbook}")
    print(f"Report period: {report_date.strftime('%Y-%m-%d')} ({period_token})")
    print(f"Mappings: {mapping_path}")
    print(f"Output dir: {output_dir}")
    print()

    qry_df = load_workbook(workbook, report_date, strict_sheets=args.strict_sheets)

    mapping_df = pd.read_csv(mapping_path)
    mapped_df = apply_mappings(qry_df, mapping_df, output_dir=str(output_dir))

    mapped_base = compose_output_name("qry_unified_mapped", current_year, timestamp, period_token, args.output_tag)
    mapped_path = output_dir / f"{mapped_base}.csv"
    mapped_df.to_csv(mapped_path, index=False)

    budget_path = project_root / f"data/inputs/budget/budget_{current_year}_processed.csv"
    prior_path = project_root / f"data/inputs/prior_years/prior_sales_{prior_year}_processed.csv"
    gvl_budget_path = project_root / f"data/inputs/budget/budget_GVL_{current_year}.csv"
    gvl_prior_path = project_root / f"data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv"
    usa_budget_path = project_root / f"data/inputs/budget/budget_USA_spa_{current_year}.csv"
    usa_prior_path = project_root / f"data/inputs/prior_years/prior_sales_{prior_year}_usa.csv"

    receivables_kwargs = {}
    if "report_date" in inspect.signature(ManagementReportGenerator.__init__).parameters:
        receivables_kwargs["report_date"] = report_date

    receivables = ManagementReportGenerator(
        str(project_root / "src/config/report_structure.json"),
        str(mapped_path),
        str(budget_path),
        str(prior_path),
        **receivables_kwargs,
    )
    apply_report_date_anchor(receivables, report_date)
    receivables_df = receivables.calculate_report()
    receivables.render_report(receivables_df)

    usa_kwargs = {}
    if "report_date" in inspect.signature(USASpaReportGenerator.__init__).parameters:
        usa_kwargs["report_date"] = report_date

    usa = USASpaReportGenerator(
        str(project_root / "src/config/usa_spa_report_structure.json"),
        str(mapped_path),
        str(usa_budget_path),
        str(usa_prior_path),
        **usa_kwargs,
    )
    apply_report_date_anchor(usa, report_date)
    usa_df = usa.calculate_report()
    usa.render_report(usa_df)

    core_kwargs = {}
    if "report_date" in inspect.signature(CoreMarketReportGenerator.__init__).parameters:
        core_kwargs["report_date"] = report_date

    core = CoreMarketReportGenerator(
        str(project_root / "src/config/core_market_report_structure.json"),
        str(mapped_path),
        str(gvl_budget_path),
        str(gvl_prior_path),
        **core_kwargs,
    )
    apply_report_date_anchor(core, report_date)
    core_df = core.calculate_report()
    core.render_report(core_df)

    usa_base = compose_output_name("management_report_usa_spa", current_year, timestamp, period_token, args.output_tag)
    core_base = compose_output_name("management_report_core_markets", current_year, timestamp, period_token, args.output_tag)
    combined_base = compose_output_name("combined_management_report", current_year, timestamp, period_token, args.output_tag)

    usa.export_report(usa_df, str(output_dir / f"{usa_base}.csv"))
    core.export_report(core_df, str(output_dir / f"{core_base}.csv"))

    separator_receivables = pd.DataFrame([{
        "label": "=== RECEIVABLES MANAGEMENT REPORT ===",
        "sales": 0.0,
        "budget": 0.0,
        "prior": 0.0,
        "is_spacer": True,
        "is_total": False,
        "is_grand_total": False,
    }])
    separator_usa = pd.DataFrame([{
        "label": "=== USA SPA REGIONAL REPORT ===",
        "sales": 0.0,
        "budget": 0.0,
        "prior": 0.0,
        "is_spacer": True,
        "is_total": False,
        "is_grand_total": False,
    }])

    usa_as_sales = usa_df.rename(columns={"actual": "sales"})
    combined_df = pd.concat([separator_receivables, receivables_df, separator_usa, usa_as_sales], ignore_index=True)
    receivables.export_report(combined_df, str(output_dir / f"{combined_base}.csv"))

    print()
    print("[OK] V2 generation complete")
    print(f"[OK] Mapped file: {mapped_path.name}")
    print(f"[OK] USA base: {usa_base}")
    print(f"[OK] Core base: {core_base}")
    print(f"[OK] Combined base: {combined_base}")


if __name__ == "__main__":
    main()
