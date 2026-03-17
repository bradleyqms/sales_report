import calendar
import datetime
from typing import Iterable, Optional

import pandas as pd


class ValidationError(ValueError):
    """Raised when strict V2 validation fails."""


def _last_business_day(year: int, month: int) -> datetime.date:
    last_day = calendar.monthrange(year, month)[1]
    candidate = datetime.date(year, month, last_day)
    while candidate.weekday() >= 5:
        candidate -= datetime.timedelta(days=1)
    return candidate


def _normalize_report_type(report_type: str) -> str:
    value = (report_type or "").strip().upper()
    if value not in {"MTD", "EOM"}:
        raise ValidationError(f"Unsupported report_type: {report_type}")
    return value


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> None:
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValidationError(f"Missing required columns: {missing}")


def validate_critical_nulls(
    df: pd.DataFrame,
    critical_columns: Iterable[str],
    max_null_ratio: float = 0.0,
) -> None:
    if df.empty:
        raise ValidationError("Input dataframe is empty")

    for col in critical_columns:
        if col not in df.columns:
            continue
        null_ratio = df[col].isna().mean()
        if null_ratio > max_null_ratio:
            raise ValidationError(
                f"Column '{col}' exceeds null threshold: {null_ratio:.2%} > {max_null_ratio:.2%}"
            )


def validate_allowed_values(df: pd.DataFrame, column: str, allowed_values: Iterable[str]) -> None:
    if column not in df.columns:
        raise ValidationError(f"Column '{column}' not found for domain validation")

    allowed = {str(v).strip().upper() for v in allowed_values}
    observed = (
        df[column]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
    )

    invalid = sorted(set(observed.unique()) - allowed)
    if invalid:
        raise ValidationError(f"Column '{column}' has invalid values: {invalid}")


def coerce_extract_date_column(df: pd.DataFrame, date_col: str = "Extract_Date") -> pd.Series:
    if date_col not in df.columns:
        raise ValidationError(f"Date column '{date_col}' is required")

    raw_values = df[date_col]
    raw_as_str = raw_values.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    compact_mask = raw_as_str.str.fullmatch(r"\d+")

    parsed = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    if compact_mask.any():
        compact_text = raw_as_str[compact_mask]
        if not compact_text.str.fullmatch(r"\d{8}").all():
            bad_values = compact_text.loc[~compact_text.str.fullmatch(r"\d{8}")].unique().tolist()[:5]
            raise ValidationError(
                "Numeric extract dates must be strict 8-digit YYYYMMDD values. "
                f"Found invalid values: {bad_values}"
            )
        parsed_compact = pd.to_datetime(compact_text, format="%Y%m%d", errors="coerce")
        parsed.loc[compact_mask] = parsed_compact

    non_compact_mask = ~compact_mask
    if non_compact_mask.any():
        parsed.loc[non_compact_mask] = pd.to_datetime(raw_as_str[non_compact_mask], errors="coerce")

    if parsed.isna().all():
        raise ValidationError(f"Could not parse any valid dates from '{date_col}'")

    return parsed


def validate_period_completeness(
    df: pd.DataFrame,
    report_type: str,
    report_date: datetime.datetime,
    date_col: str = "Extract_Date",
    strict: bool = True,
    eom_policy: str = "calendar-day",
) -> Optional[str]:
    mode = _normalize_report_type(report_type)
    parsed_dates = coerce_extract_date_column(df, date_col=date_col)

    expected_year = report_date.year
    expected_month = report_date.month

    month_mask = (parsed_dates.dt.year == expected_year) & (parsed_dates.dt.month == expected_month)
    if not month_mask.any():
        msg = (
            f"No rows in expected period {expected_year}-{expected_month:02d} "
            f"for report_type={mode}"
        )
        if strict:
            raise ValidationError(msg)
        return msg

    period_dates = parsed_dates[month_mask].dropna()
    max_seen = period_dates.max()

    if mode == "EOM":
        policy = (eom_policy or "").strip().lower()
        if policy not in {"calendar-day", "business-day"}:
            raise ValidationError(
                f"Unsupported eom_policy: {eom_policy}. Use calendar-day or business-day."
            )

        if policy == "business-day":
            expected_date = _last_business_day(expected_year, expected_month)
        else:
            month_end_day = calendar.monthrange(expected_year, expected_month)[1]
            expected_date = datetime.date(expected_year, expected_month, month_end_day)

        if max_seen.date() != expected_date:
            msg = (
                f"EOM completeness check failed: max Extract_Date={max_seen.date()} "
                f"expected={expected_date} (policy={policy})"
            )
            if strict:
                raise ValidationError(msg)
            return msg

    if mode == "MTD":
        if max_seen.date() > report_date.date():
            msg = (
                f"MTD completeness check failed: max Extract_Date={max_seen.date()} "
                f"is after report_date={report_date.date()}"
            )
            if strict:
                raise ValidationError(msg)
            return msg

    return None


def run_ingestion_validations(
    canonical_df: pd.DataFrame,
    report_type: str,
    report_date: datetime.datetime,
    strict: bool = True,
    eom_policy: str = "calendar-day",
) -> list[str]:
    warnings = []

    required = [
        "Sales Employee Name",
        "Customer Name",
        "Total Value (EUR)",
        "Document Type",
        "Company Entity",
        "Currency",
        "Source_File",
        "Extract_Date",
        "Value_in_EUR_converted",
    ]
    validate_required_columns(canonical_df, required)

    validate_critical_nulls(
        canonical_df,
        critical_columns=["Document Type", "Company Entity", "Currency", "Extract_Date"],
        max_null_ratio=0.0,
    )

    validate_allowed_values(canonical_df, "Document Type", ["AR", "CN", "SO_OPEN", "SO_TOTAL"])
    validate_allowed_values(canonical_df, "Currency", ["EUR", "USD", "CHF", "GBP"])

    warning = validate_period_completeness(
        canonical_df,
        report_type=report_type,
        report_date=report_date,
        strict=strict,
        eom_policy=eom_policy,
    )
    if warning:
        warnings.append(warning)

    return warnings
