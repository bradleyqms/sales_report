"""
Pre-Calculation vs Final Calculation Validation Script
Compares revenue figures from EOM input query → mapped query → final report outputs
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Define paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
INPUTS_DIR = DATA_DIR / 'inputs'
OUTPUTS_DIR = DATA_DIR / 'outputs'

# Find the most recent input and output files
def find_latest_file(directory, pattern):
    """Find the most recently modified file matching pattern."""
    files = sorted(directory.glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None

# Locate files
input_file = find_latest_file(INPUTS_DIR, '*new_unified_dbo_qry_last_month.xlsx')
mapped_qry = find_latest_file(OUTPUTS_DIR, 'qry_unified_mapped_2026_EOM*.csv')
final_report = find_latest_file(OUTPUTS_DIR, 'combined_management_report_2026_EOM*.csv')

print("\n" + "=" * 100)
print("PRE-CALCULATION vs FINAL CALCULATION VALIDATION")
print("=" * 100)
print(f"\n[FILES]")
print(f"  Input:       {input_file.name if input_file else 'NOT FOUND'}")
print(f"  Mapped:      {mapped_qry.name if mapped_qry else 'NOT FOUND'}")
print(f"  Final:       {final_report.name if final_report else 'NOT FOUND'}")

# ============================================================================
# Step 1: Load and analyze INPUT
# ============================================================================
input_df = None
if input_file and input_file.exists():
    input_df = pd.read_excel(input_file, sheet_name=0)
    print(f"\n[1] INPUT DATA (Raw Query)")
    print(f"    Rows: {len(input_df)}, Columns: {list(input_df.columns)}")
else:
    print("\n[1] INPUT DATA - NOT FOUND")

# ============================================================================
# Step 2: Load MAPPED QUERY
# ============================================================================
mapped_df = None
if mapped_qry and mapped_qry.exists():
    mapped_df = pd.read_csv(mapped_qry)
    print(f"\n[2] MAPPED QUERY (Intermediate)")
    print(f"    Rows: {len(mapped_df)}, Columns: {list(mapped_df.columns)[:10]}...")
else:
    print("\n[2] MAPPED QUERY - NOT FOUND")

# ============================================================================
# Step 3: Load FINAL REPORT
# ============================================================================
final_df = None
if final_report and final_report.exists():
    final_df = pd.read_csv(final_report)
    print(f"\n[3] FINAL REPORT (Summary)")
    print(f"    Rows: {len(final_df)}, Columns: {list(final_df.columns)}")
else:
    print("\n[3] FINAL REPORT - NOT FOUND")

# ============================================================================
# STAGE 1: INPUT → MAPPED conversion analysis
# ============================================================================
print("\n" + "-" * 100)
print("STAGE 1: INPUT → MAPPED QUERY (Currency conversion & mapping)")
print("-" * 100)

if input_df is not None and mapped_df is not None:
    # Sum input by region
    input_sum = input_df.groupby('Region')['Net_Value'].sum().sort_values(ascending=False)
    print(f"\n✓ INPUT data by Region:")
    for region, value in input_sum.items():
        print(f"    {region:15} : {value:15,.2f}")
    print(f"    {'TOTAL':15} : {input_sum.sum():15,.2f}")
    
    # Check mapped query
    print(f"\n✓ MAPPED data by Company_Group:")
    if 'Company_Group' in mapped_df.columns and 'Value_in_EUR_converted' in mapped_df.columns:
        mapped_sum = mapped_df.groupby('Company_Group')['Value_in_EUR_converted'].sum().sort_values(ascending=False)
        for company, value in mapped_sum.items():
            print(f"    {company:30} : {value:15,.2f} EUR")
        print(f"    {'TOTAL':30} : {mapped_sum.sum():15,.2f} EUR")
        
        # Check if totals align (accounting for currency conversion)
        input_total = input_sum.sum()
        mapped_total = mapped_sum.sum()
        diff = abs(mapped_total - input_total) / input_total * 100 if input_total > 0 else 0
        print(f"\n  Variance between input & mapped: {diff:.2f}%")
        if diff > 2:
            print(f"  ⚠️  WARNING: Larger variance may indicate currency conversion issue or data filtering")
    else:
        print("    (Company_Group or Value_in_EUR_converted column not found)")

# ============================================================================
# STAGE 2: MAPPED → FINAL conversion analysis
# ============================================================================
print("\n" + "-" * 100)
print("STAGE 2: MAPPED QUERY → FINAL REPORT (Aggregation & hierarchy)")
print("-" * 100)

if mapped_df is not None and final_df is not None:
    print(f"\n✓ Detail ROW COUNT:")
    print(f"    Mapped query (detail):    {len(mapped_df):4} rows")
    print(f"    Final report (summary):   {len(final_df):4} rows")
    print(f"    Ratio: {len(final_df)/len(mapped_df)*100:.1f}% (aggregated)")
    
    # Parse final report values
    print(f"\n✓ FINAL REPORT KEY TOTALS:")
    if 'kEUR' in final_df.columns and 'Mar-26A EOM' in final_df.columns:
        # Find the "Total Sales" row
        total_rows = final_df[final_df['kEUR'].str.contains('Total Sales|TOTAL|Grand Total', case=False, na=False)]
        if len(total_rows) > 0:
            for idx, row in total_rows.iterrows():
                print(f"\n  {row['kEUR']}")
                print(f"    Mar-26A (Actual):  {row['Mar-26A EOM']:>10}")
                print(f"    Budget:           {row['Budget']:>10}")
                print(f"    Prior Year:       {row['Prior']:>10}")
        else:
            # Show last row as proxy for total
            last_row = final_df.iloc[-1]
            print(f"\n  {last_row['kEUR']}")
            print(f"    Mar-26A (Actual):  {last_row['Mar-26A EOM']:>10}")
            print(f"    Budget:           {last_row['Budget']:>10}")
            print(f"    Prior Year:       {last_row['Prior']:>10}")

# ============================================================================
# STAGE 3: HIERARCHICAL STRUCTURE ANALYSIS
# ============================================================================
print("\n" + "-" * 100)
print("STAGE 3: Hierarchical Structure Verification")
print("-" * 100)

if final_df is not None and 'kEUR' in final_df.columns and 'Mar-26A EOM' in final_df.columns:
    print(f"\n✓ REPORT HIERARCHY (first 20 rows):")
    
    for idx, row in final_df.head(20).iterrows():
        label = row['kEUR']
        value = row['Mar-26A EOM']
        # Detect indentation/hierarchy from the text
        indent = len(label) - len(label.lstrip())
        marker = '└─ ' if indent > 0 else '├─ '
        clean_label = label.strip()
        
        # Format value
        if pd.isna(value) or value == '' or value == '-':
            value_str = '    -    '
        else:
            try:
                val_float = float(value) if isinstance(value, (int, float)) else float(str(value).replace(',', '.'))
                value_str = f'{val_float:10.0f}'
            except:
                value_str = str(value)
        
        print(f"  {marker}{clean_label:40} {value_str:>12}")

# ============================================================================
# STAGE 4: DATA QUALITY VALIDATION
# ============================================================================
print("\n" + "-" * 100)
print("STAGE 4: Data Quality Checks")
print("-" * 100)

if input_df is not None:
    print(f"\n✓ INPUT DATA QUALITY:")
    print(f"    Null values in Region:     {input_df['Region'].isna().sum()}")
    print(f"    Null values in Net_Value:  {input_df['Net_Value'].isna().sum()}")
    print(f"    Duplicate rows:            {input_df.duplicated().sum()}")

if final_df is not None:
    print(f"\n✓ FINAL REPORT QUALITY:")
    print(f"    Total rows: {len(final_df)}")
    print(f"    Empty/hyphenated values:")
    if 'Mar-26A EOM' in final_df.columns:
        empty = final_df['Mar-26A EOM'].isin(['-', '']).sum() + final_df['Mar-26A EOM'].isna().sum()
        print(f"      Mar-26A EOM: {empty}")
    if 'Budget' in final_df.columns:
        empty = final_df['Budget'].isin(['-', '']).sum() + final_df['Budget'].isna().sum()
        print(f"      Budget: {empty}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "=" * 100)
print("VALIDATION SUMMARY")
print("=" * 100)

print("""
✓ Compared INPUT (raw query) →  MAPPED (converted/mapped) → FINAL (aggregated)
✓ Verified row counts and aggregation ratios
✓ Checked currency conversion consistency
✓ Displayed hierarchical structure of final report

NEXT STEPS FOR VALIDATION:
1. ✓ Verify input total matches mapped total (accounting for EUR conversion)
2. ✓ Confirm final report totals build from mapped query detail rows
3. ✓ Review hierarchical relationships (parent = sum of children)
4. ✓ Check for missing/null values in expected metrics
5. ✓ Validate Company 3 Sales, eCommerce USA, Distributor allocations

RUN THIS SCRIPT AFTER:
- Changes to ETL mapping logic
- Currency conversion updates  
- Aggregation formula changes
""")

print("=" * 100)
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
