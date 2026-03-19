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
    """Build a combined dataframe from three separate report dataframes.
    
    The structure is: receivables (including "Total Sales") → USA SPA → CORE MARKETS.
    The dispatch email builder uses "Total Sales" as a split point to create separate
    tables for Management Report and USA SPA Regional Breakdown within the Management email.
    """
    usa_df_for_combined = usa_spa_df.rename(columns={"actual": "sales"}).copy()
    
    # Build the combined dataframe directly without spacer rows that might interfere
    # with HTML table split detection in dispatch.
    # The receivables_df contains "Total Sales" as its last row (is_grand_total=True)
    # which is the split point the dispatch HTML builder looks for.
    core_marker = pd.DataFrame([
        {
            "label": "=== CORE MARKET REPORT ===",
            "sales": 0.0,
            "budget": 0.0,
            "prior": 0.0,
            "is_spacer": False,
            "is_total": False,
            "is_grand_total": False,
        }
    ])

    parts = [
        receivables_df,
        usa_df_for_combined,
        core_marker,
        core_market_df,
    ]

    combined = pd.concat(parts, ignore_index=True)
    
    # Ensure all required columns exist with appropriate defaults
    # This prevents NaN misalignment when source dataframes have different column sets
    for col in ['sales', 'budget', 'prior', 'is_spacer', 'is_total', 'is_grand_total', 'label']:
        if col not in combined.columns:
            if col in ['is_spacer', 'is_total', 'is_grand_total']:
                combined[col] = False
            elif col == 'label':
                combined[col] = ''
            else:
                combined[col] = 0.0
    
    # Explicitly set is_spacer to False for all rows (no spacer rows in combined export)
    if 'is_spacer' in combined.columns:
        combined['is_spacer'] = False
    
    return combined
