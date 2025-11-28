#!/usr/bin/env python3
"""Print unmapped customer records (non-GmbH/AG) using the project's mapping logic.

This script will:
- locate `data/outputs/qry_unified_2025.csv` as the sales input (same as `qry_data_mapping` expects)
- locate `data/inputs/mappings/entity_mappings.csv` as the mapping file
- run `apply_mappings` from `src/qry_data_mapping.py`
- print the count and list of unmapped customer rows (where Market_Group is missing for non-GmbH/AG)

Usage:
  python scripts/print_unmapped.py [--limit N] [--show-all]

--limit N    Limit number of rows printed (default: 200). Use 0 for no limit.
--show-all    Print all columns for unmapped rows (default: prints selected key columns).
"""

from pathlib import Path
import sys
import pandas as pd

def find_project_root():
    # scripts/ is under the project root -> parent.parent
    return Path(__file__).resolve().parent.parent

def load_inputs(project_root: Path):
    inputs_folder = project_root / 'data' / 'inputs'
    outputs_folder = project_root / 'data' / 'outputs'

    mapping_file = inputs_folder / 'mappings' / 'entity_mappings.csv'
    sales_file = outputs_folder / 'qry_unified_2025.csv'

    if not sales_file.exists():
        raise FileNotFoundError(f"Sales input not found: {sales_file}")
    if not mapping_file.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

    sales_df = pd.read_csv(sales_file)
    mapping_df = pd.read_csv(mapping_file)
    return sales_df, mapping_df

def print_unmapped(sales_df: pd.DataFrame, mapping_df: pd.DataFrame, limit: int = 200, show_all: bool = False):
    # Ensure project src is on sys.path so we can import the mapping function
    project_root = find_project_root()
    src_path = str(project_root / 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    try:
        from qry_data_mapping import apply_mappings
    except Exception as e:
        raise RuntimeError(f"Could not import apply_mappings from project src: {e}")

    mapped = apply_mappings(sales_df.copy(), mapping_df.copy())

    # Unmapped customers: non-GmbH/AG & Market_Group is NaN
    mask = (~mapped['Company Entity'].isin(['GmbH', 'AG'])) & (mapped['Market_Group'].isna())
    unmapped = mapped[mask].copy()

    count = len(unmapped)
    print(f"Found {count} unmapped customer records (non-GmbH/AG)")

    if count == 0:
        return

    # Select columns to print by default
    default_cols = [c for c in ['Customer Name', 'Customer Number', 'Company Entity', 'Document Number', 'Sales Employee Name', 'Value_in_EUR_converted', 'Market_Group'] if c in unmapped.columns]

    if show_all:
        to_print = unmapped
    else:
        to_print = unmapped[default_cols]

    if limit > 0:
        print_rows = to_print.head(limit)
    else:
        print_rows = to_print

    # Print as a simple table
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    print(print_rows.to_string(index=False))

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='List unmapped customer records using project mapping logic')
    parser.add_argument('--limit', type=int, default=200, help='Max rows to print (0 = no limit)')
    parser.add_argument('--show-all', action='store_true', help='Show all columns for unmapped rows')
    args = parser.parse_args()

    try:
        project_root = find_project_root()
        sales_df, mapping_df = load_inputs(project_root)
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    try:
        print_unmapped(sales_df, mapping_df, limit=args.limit, show_all=args.show_all)
    except Exception as e:
        print(f"[ERROR] Failed to compute unmapped records: {e}")
        raise

