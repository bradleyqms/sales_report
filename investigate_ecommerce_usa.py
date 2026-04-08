#!/usr/bin/env python3
"""Investigate missing eCommerce USA data in EOM report"""
import pandas as pd
from pathlib import Path

# Check the input Excel file
input_file = Path("data/inputs/20260403T103752.253-new_unified_dbo_qry_last_month.xlsx")
print("="*80)
print("INVESTIGATING MISSING eCommerce USA DATA")
print("="*80)
print(f"\nInput file: {input_file.name}")
print(f"Exists: {input_file.exists()}")

if input_file.exists():
    # Read the Excel file
    df = pd.read_excel(input_file)
    print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")
    
    # Look for eCommerce USA entries
    print("\n" + "-"*80)
    print("SEARCHING FOR eCommerce USA DATA IN INPUT FILE")
    print("-"*80)
    
    if 'Entity_Name' in df.columns and 'Region' in df.columns:
        # Search for eCommerce USA
        ecommerce_usa = df[
            (df['Entity_Name'].str.contains('eCommerce', case=False, na=False)) &
            (df['Region'].str.contains('USA', case=False, na=False))
        ]
        if len(ecommerce_usa) > 0:
            print(f"\n✓ Found {len(ecommerce_usa)} eCommerce USA rows in INPUT FILE:")
            print(ecommerce_usa[['Region', 'Entity_Name', 'Entity_Code', 'Net_Value']].to_string())
        else:
            print(f"\n✗ No eCommerce USA rows found in input file")
    
    # Look for all eCommerce rows
    if 'Entity_Name' in df.columns:
        ecommerce_rows = df[df['Entity_Name'].str.contains('eCommerce', case=False, na=False)]
        if len(ecommerce_rows) > 0:
            print(f"\n\nAll eCommerce rows in input ({len(ecommerce_rows)} total):")
            print(ecommerce_rows[['Region', 'Entity_Name', 'Net_Value']].to_string())
    
    # Look for Shopify specifically
    if 'Entity_Code' in df.columns:
        shopify_rows = df[df['Entity_Code'].str.contains('Shopify', case=False, na=False)]
        if len(shopify_rows) > 0:
            print(f"\n\nShopify rows in input ({len(shopify_rows)} total):")
            print(shopify_rows[['Region', 'Entity_Name', 'Entity_Code', 'Net_Value']].to_string())
    
    # Check the actual row mentioned
    print("\n" + "-"*80)
    print("CHECKING FOR SPECIFIC ROW: US, USD, Shopify, 29673.16")
    print("-"*80)
    
    if 'Currency' in df.columns:
        target_row = df[
            (df['Region'] == 'US') &
            (df.get('Currency', '') == 'USD') &
            (df['Entity_Code'].str.contains('Shopify', case=False, na=False)) |
            (df['Net_Value'] == 29673.16)
        ]
        if len(target_row) > 0:
            print(f"\n✓ Found matching row(s):")
            print(target_row.to_string())
        else:
            print(f"\n✗ Row with value 29673.16 not found")
            
            # Try broader search for this value
            if 'Net_Value' in df.columns:
                match = df[df['Net_Value'].astype(str).str.contains('29673', na=False)]
                if len(match) > 0:
                    print(f"\n  But found rows with similar value 29673:")
                    print(match[['Region', 'Entity_Name', 'Entity_Code', 'Net_Value']].to_string())

# Now check the EOM reports
print("\n" + "="*80)
print("CHECKING EOM REPORTS")
print("="*80)

reports = [
    ("Management", "data/outputs/combined_management_report_2026_EOM_20260331_eom_march_final_updated_20260403_104027.csv"),
    ("Core Markets", "data/outputs/management_report_core_markets_2026_EOM_20260331_eom_march_final_updated_20260403_104027.csv"),
    ("USA SPA", "data/outputs/management_report_usa_spa_2026_EOM_20260331_eom_march_final_updated_20260403_104027.csv")
]

for report_name, report_path in reports:
    p = Path(report_path)
    if p.exists():
        print(f"\n{report_name} Report: {p.name}")
        df_report = pd.read_csv(p)
        
        # Look for eCommerce USA
        if 'kEUR' in df_report.columns:
            ecommerce_rows = df_report[df_report['kEUR'].astype(str).str.contains('eCommerce', case=False, na=False)]
            if len(ecommerce_rows) > 0:
                print(f"  eCommerce rows found:")
                print(ecommerce_rows.to_string())
            else:
                print(f"  No eCommerce rows found in report")
                
                # Show all rows with "USA"
                usa_rows = df_report[df_report['kEUR'].astype(str).str.contains('USA', case=False, na=False)]
                if len(usa_rows) > 0:
                    print(f"\n  USA rows in report:")
                    print(usa_rows.to_string())
    else:
        print(f"\n{report_name} Report: NOT FOUND ({report_path})")
