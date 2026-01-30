import pandas as pd
from pathlib import Path
from glob import glob

# Paths to data files
project_root = Path(__file__).parent.parent
mapped_qry = project_root / "data/outputs/qry_unified_mapped_2026.csv"

print("Loading data files...")
mapped_df = pd.read_csv(mapped_qry)

print(f"Mapped QRY shape: {mapped_df.shape}")
print(f"Columns: {mapped_df.columns.tolist()}")

# Filter for Germany rows in mapped data
germany_mapped = mapped_df[mapped_df['Region'] == 'Germany'].copy()
print(f"\nGermany rows in Mapped QRY: {len(germany_mapped)}")
print(f"Germany total in Mapped QRY: {germany_mapped['Value_in_EUR_converted'].sum():.2f} EUR")

# Create analysis table
print("\n" + "="*120)
print("GERMANY SALES ANALYSIS - Mapped QRY with Sub Region Mapping Status")
print("="*120)

# Group by sales employee and sub-region to identify gaps
grouped = germany_mapped.groupby(['Sales_Employee_Cleaned', 'Sub Region']).agg({
    'Value_in_EUR_converted': 'sum',
    'Customer Name': 'count'
}).reset_index()
grouped.columns = ['Sales Employee', 'Sub Region', 'Value (EUR)', 'Transaction Count']
grouped = grouped.sort_values('Value (EUR)', ascending=False)

# Add mapping status column
grouped['Mapping Status'] = grouped['Sub Region'].apply(
    lambda x: 'MAPPED' if pd.notna(x) and str(x).strip() != '' and str(x).lower() != 'nan' else 'UNMAPPED'
)

# Format for display
display_df = grouped[['Sales Employee', 'Sub Region', 'Value (EUR)', 'Transaction Count', 'Mapping Status']].copy()
display_df['Value (EUR)'] = display_df['Value (EUR)'].apply(lambda x: f"{x:,.2f}")

print(display_df.to_string(index=False))

print("\n" + "="*120)
print("SUMMARY")
print("="*120)
mapped_val = grouped[grouped['Mapping Status'] == 'MAPPED']['Value (EUR)'].sum()
unmapped_val = grouped[grouped['Mapping Status'] == 'UNMAPPED']['Value (EUR)'].sum()
total_val = mapped_val + unmapped_val

print(f"Total Mapped Germany Sales (with Sub Region):     {mapped_val:,.2f} EUR")
print(f"Total Unmapped Germany Sales (NULL Sub Region):   {unmapped_val:,.2f} EUR")
print(f"Total Germany Sales (All):                        {total_val:,.2f} EUR")
if total_val > 0:
    print(f"\nMapping Gap: {unmapped_val:,.2f} EUR ({unmapped_val/total_val*100:.1f}%)")

# Export detailed table
output_path = project_root / "data/outputs/germany_gap_analysis.csv"
grouped.to_csv(output_path, index=False)
print(f"\nDetailed analysis saved to: {output_path}")
