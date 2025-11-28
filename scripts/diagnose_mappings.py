#!/usr/bin/env python3
"""Diagnostic script to show how unmapped customer counts change through mapping steps.

It reproduces the key steps from `src/qry_data_mapping.py` and prints counts/samples
after each transformation so you can see why the initial unmapped count is larger.
Also prints matching rows from `entity_mappings.csv` for any final unmapped customers.
"""

from pathlib import Path
import sys
import pandas as pd

def find_project_root():
    return Path(__file__).resolve().parent.parent

def load_files(root: Path):
    inputs_folder = root / 'data' / 'inputs'
    outputs_folder = root / 'data' / 'outputs'
    mapping_file = inputs_folder / 'mappings' / 'entity_mappings.csv'
    sales_file = outputs_folder / 'qry_unified_2025.csv'
    if not sales_file.exists() or not mapping_file.exists():
        raise FileNotFoundError(f"Missing files: {sales_file} or {mapping_file}")
    sales_df = pd.read_csv(sales_file)
    mapping_df = pd.read_csv(mapping_file)
    return sales_df, mapping_df

def sample_rows(df, cols=None, n=10):
    if cols:
        cols = [c for c in cols if c in df.columns]
        return df[cols].head(n)
    return df.head(n)

def diagnose():
    root = find_project_root()
    sales_df, mapping_df = load_files(root)

    print(f"Loaded sales rows: {len(sales_df)}, mapping rows: {len(mapping_df)}\n")

    # Clean mapping
    mapping_df = mapping_df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

    df = sales_df.copy()

    # EMP mapping (GmbH/AG)
    if 'Sales_Employee' in mapping_df.columns:
        emp_cols = ['Sales_Employee', 'Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
        map_emp = mapping_df[emp_cols].dropna(subset=['Sales_Employee']).drop_duplicates(subset=['Sales_Employee'])
        df['temp_employee'] = df['Sales Employee Name']
        df.loc[~df['Company Entity'].isin(['GmbH', 'AG']), 'temp_employee'] = pd.NA
        df = df.merge(map_emp, left_on='temp_employee', right_on='Sales_Employee', how='left', suffixes=('', '_emp'))

        unmapped_emp = df[df['Company Entity'].isin(['GmbH', 'AG']) & df['Market_Group'].isna()]
        print(f"After employee merge: unmapped_emp (GmbH/AG & Market_Group isna): {len(unmapped_emp)}")
        if not unmapped_emp.empty:
            print(sample_rows(unmapped_emp, cols=['Sales Employee Name', 'Customer Name', 'Company Entity', 'Market_Group'], n=5).to_string(index=False))
        df.drop('temp_employee', axis=1, inplace=True)

    # CUSTOMER mapping (non-GmbH/AG)
    if 'Customer_Name' in mapping_df.columns:
        cust_cols = ['Customer_Name', 'Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
        map_cust = mapping_df[cust_cols].dropna(subset=['Customer_Name']).drop_duplicates(subset=['Customer_Name'])

        df['temp_customer'] = df['Customer Name']
        df.loc[df['Company Entity'].isin(['GmbH', 'AG']), 'temp_customer'] = pd.NA
        df = df.merge(map_cust, left_on='temp_customer', right_on='Customer_Name', how='left', suffixes=('', '_cust'))

        # Count immediately after merge (this matches the warning in apply_mappings)
        unmapped_cust_pre = df[~df['Company Entity'].isin(['GmbH', 'AG']) & df['Market_Group'].isna()]
        print(f"After customer merge: unmapped_cust_pre (non-GmbH/AG & Market_Group isna): {len(unmapped_cust_pre)}")
        if not unmapped_cust_pre.empty:
            print(sample_rows(unmapped_cust_pre, cols=['Customer Name', 'Company Entity', 'Market_Group', 'Market_Group_cust'], n=10).to_string(index=False))

        # Combine mapping columns (fill from _cust)
        common_cols = ['Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
        for col in common_cols:
            if col + '_cust' in df.columns:
                df[col] = df[col].fillna(df[col + '_cust'])
                df.drop(col + '_cust', axis=1, inplace=True)

        # Recompute unmapped after combine
        unmapped_postcombine = df[~df['Company Entity'].isin(['GmbH', 'AG']) & df['Market_Group'].isna()]
        print(f"After combining mapping cols: unmapped_postcombine (non-GmbH/AG & Market_Group isna): {len(unmapped_postcombine)}")
        if not unmapped_postcombine.empty:
            print(sample_rows(unmapped_postcombine, cols=['Customer Name', 'Company Entity', 'Market_Group'], n=10).to_string(index=False))

        # Drop merge keys if present
        for col in ['Sales_Employee', 'Customer_Name']:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)

    # Apply downstream filters from apply_mappings
    before_filters = len(df)

    # Export entity filter: keep only AR rows for Export entity
    df = df[~((df['Company Entity'] == 'Export') & (df['Document Type'] != 'AR'))]
    after_export = len(df)
    print(f"After Export non-AR filter: rows {before_filters} -> {after_export} (removed {before_filters - after_export})")

    # Switzerland region rule
    if 'Region' in df.columns:
        before_sw = len(df)
        df = df[~((df['Region'] == 'Switzerland') & (df['Company Entity'] != 'AG'))]
        after_sw = len(df)
        print(f"After Switzerland rule: rows {before_sw} -> {after_sw} (removed {before_sw - after_sw})")

    # Filter out 'Interco' in Customer Name
    before_interco = len(df)
    df = df[~df['Customer Name'].str.contains('Interco', case=False, na=False)]
    after_interco = len(df)
    print(f"After Interco filter: rows {before_interco} -> {after_interco} (removed {before_interco - after_interco})")

    # Final unmapped
    final_unmapped = df[~df['Company Entity'].isin(['GmbH', 'AG']) & df['Market_Group'].isna()]
    print(f"Final unmapped (post-filters): {len(final_unmapped)}")
    if not final_unmapped.empty:
        print(final_unmapped.to_string(index=False))

    # Show mapping file rows for these customers
    if not final_unmapped.empty:
        cust_names = final_unmapped['Customer Name'].dropna().unique().tolist()
        mapping_rows = mapping_df[mapping_df['Customer_Name'].isin(cust_names)] if 'Customer_Name' in mapping_df.columns else pd.DataFrame()
        print(f"\nMapping file rows for final unmapped customers: {len(mapping_rows)}")
        if not mapping_rows.empty:
            print(mapping_rows.to_string(index=False))

if __name__ == '__main__':
    try:
        diagnose()
    except Exception as e:
        print(f"[ERROR] Diagnosis failed: {e}")
        raise
