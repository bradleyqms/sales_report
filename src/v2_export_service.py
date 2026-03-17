import datetime
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


@dataclass
class V2ExportService:
    exported_paths: set[str] = field(default_factory=set)

    def export_once(self, generator, df: pd.DataFrame, csv_path: Path) -> bool:
        resolved = str(csv_path.resolve())
        if resolved in self.exported_paths:
            return False
        generator.export_report(df, str(csv_path))
        self.exported_paths.add(resolved)
        return True


def build_period_token(report_type: str, report_date: datetime.datetime) -> str:
    mode = (report_type or "").strip().upper()
    if mode not in {"MTD", "EOM"}:
        raise ValueError(f"Unsupported report_type: {report_type}")
    return f"{mode}_{report_date.strftime('%Y%m%d')}"


def compose_output_name(
    prefix: str,
    year: int,
    timestamp: str,
    period_token: str,
    output_tag: str | None,
) -> str:
    parts = [prefix, str(year), period_token]
    if output_tag and str(output_tag).strip():
        parts.append(str(output_tag).strip())
    parts.append(timestamp)
    return "_".join(parts)


def build_combined_dataframe(
    receivables_df: pd.DataFrame,
    usa_spa_df: pd.DataFrame,
    core_market_df: pd.DataFrame,
) -> pd.DataFrame:
    separator_receivables = pd.DataFrame([
        {
            "label": "=== RECEIVABLES MANAGEMENT REPORT ===",
            "sales": 0.0,
            "budget": 0.0,
            "prior": 0.0,
            "is_spacer": True,
            "is_total": False,
            "is_grand_total": False,
        }
    ])

    separator_usa = pd.DataFrame([
        {
            "label": "=== USA SPA REGIONAL REPORT ===",
            "sales": 0.0,
            "budget": 0.0,
            "prior": 0.0,
            "is_spacer": True,
            "is_total": False,
            "is_grand_total": False,
        }
    ])

    separator_core = pd.DataFrame([
        {
            "label": "=== CORE MARKET REPORT ===",
            "sales": 0.0,
            "budget": 0.0,
            "prior": 0.0,
            "is_spacer": True,
            "is_total": False,
            "is_grand_total": False,
        }
    ])

    usa_df_for_combined = usa_spa_df.rename(columns={"actual": "sales"}).copy()

    parts = [
        separator_receivables,
        receivables_df,
        separator_usa,
        usa_df_for_combined,
        separator_core,
        core_market_df,
    ]

    return pd.concat(parts, ignore_index=True)
