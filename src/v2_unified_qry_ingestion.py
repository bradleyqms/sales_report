import datetime
import json
import logging
from pathlib import Path
import re
from typing import Optional

import pandas as pd


FX_RATES = {"CHF": 1.08, "USD": 0.96, "GBP": 1.20, "EUR": 1.00}

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
    "Sales Employee Name": ["Sales Employee Name", "Sales Employee", "Salesperson", "SlpName", "Employee"],
    "Customer Name": ["Customer Name", "Customer", "CardName", "Customer_Name", "Account Name"],
    "Total Value (EUR)": [
        "Total Value (EUR)",
        "Total Value",
        "Net Value",
        "Net_Value",
        "Amount",
        "Sales Amount",
        "Value",
    ],
    "Document Type": ["Document Type", "Doc Type", "Type", "Document"],
    "Customer Code": ["Customer Code", "Customer No", "Customer Number", "CardCode", "Entity_Code"],
    "Extract_Date": ["Extract_Date", "Extract Date", "Extract_Date_Int", "Posting Date", "Date"],
    "Currency": ["Currency", "Curr", "ISO Currency"],
    "Region": ["Region", "Company Entity", "Entity"],
    "Entity_Type": ["Entity_Type", "Entity Type"],
    "Entity_Name": ["Entity_Name", "Entity Name"],
}

DEFAULT_SCHEMA_MANIFEST = Path(__file__).resolve().parent / "config" / "schema_profiles_v1.json"
UNIT_SEPARATOR = "\x1f"
ESCAPED_UNIT_SEPARATORS = ("\\x1f", "\\u001f")


def _fallback_schema_profiles() -> dict:
    return {
        "version": "fallback-v1",
        "profiles": {
            "MTD": {
                "required": {
                    "Region": ["Region"],
                    "Entity_Type": ["Entity_Type", "Entity Type"],
                    "Entity_Name": ["Entity_Name", "Entity Name"],
                    "Net_Value": ["Net_Value", "Net Value"],
                    "Currency": ["Currency"],
                    "Extract_Date_Int": ["Extract_Date_Int", "Extract_Date", "Extract Date"],
                    "Entity_Code": ["Entity_Code", "Entity Code", "Customer Code"],
                    "Document_Type": ["Document_Type", "Document Type", "Doc Type"],
                }
            },
            "EOM": {
                "required": {
                    "Region": ["Region"],
                    "Entity_Type": ["Entity_Type", "Entity Type"],
                    "Entity_Name": ["Entity_Name", "Entity Name"],
                    "Net_Value": ["Net_Value", "Net Value"],
                    "Currency": ["Currency"],
                    "Extract_Date_Int": ["Extract_Date_Int", "Extract_Date", "Extract Date"],
                    "Entity_Code": ["Entity_Code", "Entity Code", "Customer Code"],
                    "Document_Type": ["Document_Type", "Document Type", "Doc Type"],
                }
            },
        },
    }


def normalize_col_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def find_matching_column(df: pd.DataFrame, aliases: list[str]) -> Optional[str]:
    normalized = {normalize_col_name(col): col for col in df.columns}
    for alias in aliases:
        key = normalize_col_name(alias)
        if key in normalized:
            return normalized[key]
    return None


def _resolve_report_date(report_date: Optional[datetime.datetime]) -> datetime.datetime:
    if report_date is not None:
        return report_date
    now = datetime.datetime.now()
    return datetime.datetime(now.year, now.month, now.day)


def _coerce_extract_date_series(source_series: pd.Series, fallback_date: datetime.datetime) -> pd.Series:
    as_text = source_series.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    compact_mask = as_text.str.fullmatch(r"\d{8}")
    if not compact_mask.all():
        bad_values = as_text.loc[~compact_mask].dropna().unique().tolist()
        preview = bad_values[:5]
        raise ValueError(
            "Extract_Date_Int must be strict 8-digit YYYYMMDD values. "
            f"Found invalid values: {preview}"
        )

    parsed = pd.to_datetime(as_text, format="%Y%m%d", errors="coerce")
    if parsed.isna().any():
        bad_values = as_text.loc[parsed.isna()].dropna().unique().tolist()
        preview = bad_values[:5]
        raise ValueError(
            "Extract_Date_Int contains unparseable YYYYMMDD values. "
            f"Found invalid values: {preview}"
        )

    return parsed


def _normalize_document_sign(df: pd.DataFrame) -> None:
    doc_upper = df["Document Type"].astype(str).str.strip().str.upper()
    values = pd.to_numeric(df["Total Value (EUR)"], errors="coerce").fillna(0.0)

    cn_mask = doc_upper == "CN"
    ar_mask = doc_upper == "AR"

    values.loc[cn_mask] = -values.loc[cn_mask].abs()
    values.loc[ar_mask] = values.loc[ar_mask].abs()

    df["Document Type"] = doc_upper
    df["Total Value (EUR)"] = values


def _read_unified_source_csv(src: Path) -> pd.DataFrame:
    """
        Strict parser for SAP unified export encoded with ASCII Unit Separator (0x1F).

        Accepted headers (with trailing separator tolerated):
            Region, Currency, Extract_Date_Int, Entity_Type, Entity_Code, Entity_Name, Net_Value
            Region, Currency, Extract_Date_Int, Entity_Type, Entity_Code, Entity_Name, Net_Value, Document_Type

        Any malformed row is a hard failure.
    """
    EXPECTED_COLS = [
        "Region", "Currency", "Extract_Date_Int", "Entity_Type",
        "Entity_Code", "Entity_Name", "Net_Value", "Document_Type",
    ]
    BASE_HEADER = [
        "Region",
        "Currency",
        "Extract_Date_Int",
        "Entity_Type",
        "Entity_Code",
        "Entity_Name",
        "Net_Value",
    ]

    rows: list[dict] = []

    with open(src, "r", encoding="utf-8-sig", errors="replace") as fh:
        all_lines = fh.readlines()

    # Some upstream exports may contain escaped separators ("\\x1f" / "\\u001f")
    # instead of the raw ASCII unit-separator byte. Normalize those first.
    if all_lines and UNIT_SEPARATOR not in all_lines[0]:
        has_escaped_sep = any(token in all_lines[0] for token in ESCAPED_UNIT_SEPARATORS)
        if has_escaped_sep:
            logging.warning(
                "_read_unified_source_csv: detected escaped unit separators in %s header; normalizing",
                src.name,
            )
            normalized_lines: list[str] = []
            for line in all_lines:
                fixed = line
                for token in ESCAPED_UNIT_SEPARATORS:
                    fixed = fixed.replace(token, UNIT_SEPARATOR)
                normalized_lines.append(fixed)
            all_lines = normalized_lines

    if not all_lines:
        logging.warning("_read_unified_source_csv: file is empty: %s", src)
        return pd.DataFrame(columns=EXPECTED_COLS)

    header_parts = all_lines[0].rstrip("\r\n").split(UNIT_SEPARATOR)
    if header_parts and header_parts[-1].strip() == "":
        header_parts = header_parts[:-1]

    if header_parts == BASE_HEADER:
        has_document_type = False
    elif header_parts == [*BASE_HEADER, "Document_Type"]:
        has_document_type = True
    else:
        raise ValueError(
            "Unified source header is invalid for strict mode. "
            f"Expected {BASE_HEADER} or {[*BASE_HEADER, 'Document_Type']}, got {header_parts}"
        )

    expected_parts = len(header_parts)

    # Skip the header row (first line)
    for line_no, raw in enumerate(all_lines[1:], start=2):
        line = raw.rstrip("\r\n")
        if not line.strip():
            continue

        parts = line.split(UNIT_SEPARATOR)

        # Remove a single phantom empty field produced by a trailing separator.
        if parts and parts[-1].strip() == "":
            parts = parts[:-1]

        if len(parts) != expected_parts:
            raise ValueError(
                "Unified source row has malformed field count. "
                f"line={line_no}, expected={expected_parts}, got={len(parts)}"
            )

        region       = parts[0].strip()
        currency     = parts[1].strip()
        extract_date = parts[2].strip()
        entity_type  = parts[3].strip()
        entity_code  = parts[4].strip()

        entity_name = parts[5].strip()
        net_raw = parts[6].strip()
        try:
            net_value_text = net_raw.replace(" ", "")
            if "," in net_value_text and "." in net_value_text:
                net_value_text = net_value_text.replace(".", "").replace(",", ".")
            elif "," in net_value_text:
                net_value_text = net_value_text.replace(",", ".")
            net_value: float | None = float(net_value_text)
        except (ValueError, AttributeError):
            raise ValueError(
                "Unified source row has invalid Net_Value. "
                f"line={line_no}, value='{net_raw}'"
            )

        document_type = "AR"
        if has_document_type:
            candidate = parts[7].strip()
            if candidate:
                document_type = candidate

        rows.append({
            "Region":           region,
            "Currency":         currency,
            "Extract_Date_Int": extract_date,
            "Entity_Type":      entity_type,
            "Entity_Code":      entity_code,
            "Entity_Name":      entity_name,
            "Net_Value":        net_value,
            "Document_Type":    document_type,
        })

    df = pd.DataFrame(rows, columns=EXPECTED_COLS)
    logging.info(
        "_read_unified_source_csv: loaded %d rows from %s using separator 0x1F", len(df), src.name,
    )
    return df


def _load_schema_manifest(schema_manifest_path: Optional[str] = None) -> dict:
    manifest_path = Path(schema_manifest_path).resolve() if schema_manifest_path else DEFAULT_SCHEMA_MANIFEST
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return _fallback_schema_profiles()


def get_schema_manifest_version(schema_manifest_path: Optional[str] = None) -> str:
    manifest = _load_schema_manifest(schema_manifest_path)
    return str(manifest.get("version", "unknown"))


def _resolve_strict_source_columns(
    df: pd.DataFrame,
    report_type: str,
    schema_manifest_path: Optional[str] = None,
) -> dict[str, str]:
    manifest = _load_schema_manifest(schema_manifest_path)
    mode = (report_type or "").strip().upper()
    profiles = manifest.get("profiles", {})
    if mode not in profiles:
        raise ValueError(f"No strict schema profile configured for report_type={report_type}")

    required_map = profiles[mode].get("required", {})

    resolved = {}
    missing = []
    for field, aliases in required_map.items():
        match = find_matching_column(df, aliases)
        if match is None:
            missing.append(f"{field} (aliases={aliases})")
        else:
            resolved[field] = match

    if missing:
        raise ValueError(
            "Unified source failed strict schema profile validation. "
            f"Missing fields for {mode}: {missing}"
        )

    return resolved


def load_unified_qry_csv(
    csv_path: str,
    report_type: str,
    report_date: Optional[datetime.datetime] = None,
    schema_mode: str = "strict",
    schema_manifest_path: Optional[str] = None,
) -> pd.DataFrame:
    src = Path(csv_path)
    if not src.exists():
        raise FileNotFoundError(f"Unified QRY source not found: {src}")

    mode = (report_type or "").strip().upper()
    if mode not in {"MTD", "EOM"}:
        raise ValueError(f"Unsupported report_type '{report_type}'. Expected MTD or EOM.")

    df = _read_unified_source_csv(src)
    if df.empty:
        raise ValueError(f"Unified QRY source is empty: {src}")

    fallback_date = _resolve_report_date(report_date)
    load_ts = pd.Timestamp.now()

    mode_schema = (schema_mode or "").strip().lower()
    if mode_schema not in {"strict", "flexible"}:
        raise ValueError(f"Unsupported schema_mode '{schema_mode}'. Expected strict or flexible.")

    sales_col = find_matching_column(df, COLUMN_ALIASES["Sales Employee Name"])
    customer_col = find_matching_column(df, COLUMN_ALIASES["Customer Name"])

    if mode_schema == "strict":
        strict_cols = _resolve_strict_source_columns(
            df,
            report_type=mode,
            schema_manifest_path=schema_manifest_path,
        )
        value_col = strict_cols["Net_Value"]
        doc_col = strict_cols["Document_Type"]
        code_col = strict_cols["Entity_Code"]
        date_col = strict_cols["Extract_Date_Int"]
        currency_col = strict_cols["Currency"]
        region_col = strict_cols["Region"]
        entity_type_col = strict_cols["Entity_Type"]
        entity_name_col = strict_cols["Entity_Name"]
    else:
        value_col = find_matching_column(df, COLUMN_ALIASES["Total Value (EUR)"])
        doc_col = find_matching_column(df, COLUMN_ALIASES["Document Type"])
        code_col = find_matching_column(df, COLUMN_ALIASES["Customer Code"])
        date_col = find_matching_column(df, COLUMN_ALIASES["Extract_Date"])
        currency_col = find_matching_column(df, COLUMN_ALIASES["Currency"])
        region_col = find_matching_column(df, COLUMN_ALIASES["Region"])
        entity_type_col = find_matching_column(df, COLUMN_ALIASES["Entity_Type"])
        entity_name_col = find_matching_column(df, COLUMN_ALIASES["Entity_Name"])

    if value_col is None:
        raise ValueError("Unified QRY source missing value column (Net_Value/Total Value variants)")

    out = pd.DataFrame(index=df.index)

    if entity_type_col and entity_name_col:
        entity_type = df[entity_type_col].fillna("").astype(str).str.lower().str.strip()
        entity_name = df[entity_name_col]
        is_sales = entity_type.str.contains("sales") | entity_type.str.contains("employee")
        out["Sales Employee Name"] = entity_name.where(is_sales, pd.NA)
        out["Customer Name"] = entity_name.where(~is_sales, pd.NA)
    else:
        out["Sales Employee Name"] = df[sales_col] if sales_col else pd.NA
        out["Customer Name"] = df[customer_col] if customer_col else pd.NA

    value_series = df[value_col].astype(str).str.strip()
    comma_mask = value_series.str.contains(",", regex=False)
    value_series.loc[comma_mask] = value_series.loc[comma_mask].str.replace(".", "", regex=False)
    value_series = value_series.str.replace(",", ".", regex=False)
    out["Total Value (EUR)"] = pd.to_numeric(value_series, errors="coerce").fillna(0.0)

    if doc_col:
        out["Document Type"] = df[doc_col].fillna("AR").astype(str).str.strip().replace("", "AR")
    else:
        out["Document Type"] = "AR"

    if region_col:
        region_raw = df[region_col].fillna("").astype(str).str.upper().str.strip()
        out["Company Entity"] = region_raw.map(REGION_TO_ENTITY).fillna(region_raw)
    else:
        out["Company Entity"] = "GmbH"

    if currency_col:
        out["Currency"] = df[currency_col].fillna("EUR").astype(str).str.upper().str.strip()
        out["Currency"] = out["Currency"].replace("", "EUR")
    else:
        out["Currency"] = "EUR"

    out["Customer Code"] = df[code_col] if code_col else pd.NA

    if date_col:
        out["Extract_Date"] = _coerce_extract_date_series(df[date_col], fallback_date)
    else:
        out["Extract_Date"] = pd.Timestamp(fallback_date)

    out["Source_File"] = src.name
    out["Metric"] = "Receivables"
    out["Load_Timestamp"] = load_ts

    _normalize_document_sign(out)

    out["Total Open Value (EUR)"] = out["Total Value (EUR)"]
    fx = out["Currency"].map(FX_RATES).fillna(1.0)
    out["Value_in_EUR_converted"] = out["Total Value (EUR)"] * fx

    ordered_cols = [
        "Sales Employee Name",
        "Customer Name",
        "Total Value (EUR)",
        "Document Type",
        "Company Entity",
        "Currency",
        "Customer Code",
        "Source_File",
        "Extract_Date",
        "Metric",
        "Load_Timestamp",
        "Total Open Value (EUR)",
        "Value_in_EUR_converted",
    ]
    return out[ordered_cols].copy()
