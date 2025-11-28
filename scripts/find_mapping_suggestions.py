#!/usr/bin/env python3
"""Find candidate mapping rows for given unmapped customer names using fuzzy matching.

Usage:
  python scripts/find_mapping_suggestions.py

This script looks in `data/inputs/mappings/entity_mappings.csv` and matches
against `Customer_Name` and `Sales_Employee` columns using difflib.
"""

from pathlib import Path
import difflib
import pandas as pd
import re

UNMAPPED = [
    "Liberty Professional",
    "S. Wöhrle",
    "M. Mijnheer NL",
    "Export",
    "A. Gutierrez",
    "G. van Eykern NL",
    "K. Brunbauer",
    "M. Pfauch",
]


def normalize(s):
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def main():
    project_root = Path(__file__).resolve().parent.parent
    mapping_path = project_root / 'data' / 'inputs' / 'mappings' / 'entity_mappings.csv'
    if not mapping_path.exists():
        print(f"Mapping file not found: {mapping_path}")
        return

    df = pd.read_csv(mapping_path)

    # gather candidate names from relevant columns
    candidates = set()
    cols = []
    if 'Customer_Name' in df.columns:
        cols.append('Customer_Name')
        candidates.update(df['Customer_Name'].dropna().unique())
    if 'Sales_Employee' in df.columns:
        cols.append('Sales_Employee')
        candidates.update(df['Sales_Employee'].dropna().unique())

    candidates = [c for c in candidates if str(c).strip()]
    norm_map = {c: normalize(c) for c in candidates}

    for name in UNMAPPED:
        nn = normalize(name)
        # Use difflib to get close matches on normalized forms
        scored = []
        for cand in candidates:
            score = difflib.SequenceMatcher(None, nn, norm_map[cand]).ratio()
            scored.append((score, cand))

        scored.sort(reverse=True, key=lambda x: x[0])
        top = scored[:8]

        print('\n' + '='*80)
        print(f"Unmapped: {name}")
        print("Top suggestions (score 0-1):")
        for score, cand in top:
            # show mapping rows that match this candidate
            rows = df[(df['Customer_Name'].fillna('') == cand) | (df['Sales_Employee'].fillna('') == cand)] if cols else pd.DataFrame()
            print(f"  {score:.3f}  -> {cand}")
            if not rows.empty:
                # print first matching mapping row for context
                print(rows.head(1).to_string(index=False))

    print('\nSearch complete.')


if __name__ == '__main__':
    main()
