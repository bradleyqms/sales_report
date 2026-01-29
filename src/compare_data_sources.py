#!/usr/bin/env python
"""
Compare New vs Existing Customer Breakdown file with QRY_AR_MTD_Gmbh.csv

Usage:
    python compare_data_sources.py <excel_file> <qry_csv_file> [options]

Options:
    --output OUTPUT_PATH    Save detailed comparison report to file
    --verbose              Show detailed row-by-row differences
    --summary-only         Show only summary statistics
    --match-by-customer    Group by customer instead of employee
"""

import pandas as pd
import argparse
import sys
from pathlib import Path
from datetime import datetime

def load_excel_file(file_path):
    """Load New vs Existing Customer Breakdown Excel file"""
    try:
        df = pd.read_excel(file_path)
        df['Source'] = 'Excel_NewVsExisting'
        print(f"✓ Loaded Excel file: {Path(file_path).name}")
        print(f"  Records: {len(df):,}")
        print(f"  Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"✗ Error loading Excel file: {e}")
        sys.exit(1)

def load_qry_file(file_path):
    """Load QRY_AR_MTD_Gmbh.csv file"""
    try:
        df = pd.read_csv(file_path)
        
        # Check if this is a summary file (corrupted format)
        if len(df.columns) < 3 and df.shape[1] <= 2:
            print(f"  ⚠ Warning: QRY file appears to be in summary format (not detailed)")
            print(f"  Columns detected: {list(df.columns)}")
            print(f"  This may not be the correct QRY file")
        
        df['Source'] = 'QRY_AR_Gmbh'
        print(f"✓ Loaded QRY file: {Path(file_path).name}")
        print(f"  Records: {len(df):,}")
        print(f"  Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"✗ Error loading QRY file: {e}")
        sys.exit(1)

def analyze_coverage(excel_df, qry_df):
    """Analyze how much of QRY data is covered by Excel file"""
    print("\n" + "=" * 80)
    print("COVERAGE ANALYSIS")
    print("=" * 80)
    
    # Check if QRY has expected columns
    if 'Sold-to Party' not in qry_df.columns:
        print("⚠ QRY file doesn't have expected columns")
        print(f"  Available columns: {list(qry_df.columns)}")
        print("\n  This appears to be a summary export, not the detailed QRY format")
        print("  Excel file cannot be directly compared to this QRY format")
        return
    
    # Get unique customers in each dataset
    excel_customers = set(excel_df['Customer Code'].astype(str).unique())
    qry_customers = set(qry_df['Sold-to Party'].astype(str).unique())
    
    print(f"\nUnique Customers:")
    print(f"  Excel file:     {len(excel_customers):,}")
    print(f"  QRY file:       {len(qry_customers):,}")
    
    # Find overlaps
    overlap = excel_customers & qry_customers
    only_in_excel = excel_customers - qry_customers
    only_in_qry = qry_customers - excel_customers
    
    print(f"\nCustomer Overlap:")
    print(f"  In both:        {len(overlap):,} ({len(overlap)/max(len(excel_customers), len(qry_customers))*100:.1f}%)")
    print(f"  Only in Excel:  {len(only_in_excel):,}")
    print(f"  Only in QRY:    {len(only_in_qry):,}")
    
    # Sales amount comparison
    excel_total = excel_df['MTD Sales Amount'].sum()
    
    if 'Total Value (EUR)' in qry_df.columns:
        qry_total = qry_df['Total Value (EUR)'].sum()
    else:
        print("\n⚠ Cannot calculate QRY total - column 'Total Value (EUR)' not found")
        return
    
    print(f"\nSales Volume:")
    print(f"  Excel total:    €{excel_total:>15,.2f}")
    print(f"  QRY total:      €{qry_total:>15,.2f}")
    print(f"  Difference:     €{abs(excel_total - qry_total):>15,.2f} ({abs(excel_total - qry_total)/qry_total*100:.1f}%)")
    
    # Employee comparison
    excel_employees = set(excel_df['Sales Employee'].unique())
    qry_employees = set(qry_df['Sales Employee Name'].unique())
    
    print(f"\nUnique Sales Employees:")
    print(f"  Excel file:     {len(excel_employees)}")
    print(f"  QRY file:       {len(qry_employees)}")
    print(f"  Employee overlap: {len(excel_employees & qry_employees)}")
    
    if excel_employees != (excel_employees & qry_employees):
        print(f"\n  Excel employees not in QRY:")
        for emp in sorted(excel_employees - qry_employees):
            print(f"    - {emp}")

def analyze_sales_by_employee(excel_df, qry_df):
    """Compare sales totals by employee"""
    print("\n" + "=" * 80)
    print("SALES BY EMPLOYEE")
    print("=" * 80)
    
    # Get sales by employee from Excel
    excel_by_emp = excel_df.groupby('Sales Employee')['MTD Sales Amount'].sum().sort_values(ascending=False)
    
    # Get sales by employee from QRY (approximate, as QRY has line items)
    qry_by_emp = qry_df.groupby('Sales Employee Name')['Total Value (EUR)'].sum().sort_values(ascending=False)
    
    print(f"\nTop 10 Sales Employees (Excel):")
    for emp, sales in excel_by_emp.head(10).items():
        print(f"  {emp:<30} €{sales:>12,.2f}")
    
    print(f"\nTop 10 Sales Employees (QRY):")
    for emp, sales in qry_by_emp.head(10).items():
        print(f"  {emp:<30} €{sales:>12,.2f}")
    
    # Find discrepancies
    print(f"\nEmployee Sales Comparison:")
    all_employees = set(excel_by_emp.index) | set(qry_by_emp.index)
    discrepancies = []
    
    for emp in sorted(all_employees):
        excel_sales = excel_by_emp.get(emp, 0)
        qry_sales = qry_by_emp.get(emp, 0)
        diff = excel_sales - qry_sales
        diff_pct = (diff / qry_sales * 100) if qry_sales != 0 else 0
        
        if abs(diff) > 100:  # Only show significant differences
            discrepancies.append((emp, excel_sales, qry_sales, diff, diff_pct))
    
    if discrepancies:
        print(f"\nSignificant Discrepancies (>€100):")
        print(f"  {'Employee':<30} {'Excel':>15} {'QRY':>15} {'Difference':>15} {'%':>8}")
        print(f"  {'-'*85}")
        for emp, excel_sales, qry_sales, diff, diff_pct in sorted(discrepancies, key=lambda x: abs(x[3]), reverse=True)[:15]:
            print(f"  {emp:<30} €{excel_sales:>14,.2f} €{qry_sales:>14,.2f} €{diff:>14,.2f} {diff_pct:>7.1f}%")

def analyze_customer_status(excel_df):
    """Analyze customer status distribution"""
    print("\n" + "=" * 80)
    print("CUSTOMER STATUS ANALYSIS (Excel File)")
    print("=" * 80)
    
    status_counts = excel_df['Customer Status'].value_counts()
    status_sales = excel_df.groupby('Customer Status')['MTD Sales Amount'].sum()
    
    print(f"\nCustomer Count by Status:")
    for status, count in status_counts.items():
        sales = status_sales[status]
        pct_of_total = sales / excel_df['MTD Sales Amount'].sum() * 100
        print(f"  {status:<20} {count:>5} customers  €{sales:>12,.2f} ({pct_of_total:>5.1f}%)")
    
    print(f"\nTotal: {len(excel_df):,} customers, €{excel_df['MTD Sales Amount'].sum():,.2f}")

def analyze_data_quality(excel_df, qry_df):
    """Analyze data quality and completeness"""
    print("\n" + "=" * 80)
    print("DATA QUALITY ANALYSIS")
    print("=" * 80)
    
    print(f"\nExcel File Completeness:")
    for col in excel_df.columns:
        if col == 'Source':
            continue
        null_count = excel_df[col].isna().sum()
        null_pct = null_count / len(excel_df) * 100
        status = "✓" if null_pct == 0 else "✗"
        print(f"  {status} {col:<25} {null_count:>5} nulls ({null_pct:>5.1f}%)")
    
    print(f"\nQRY File Completeness:")
    for col in qry_df.columns:
        if col == 'Source':
            continue
        null_count = qry_df[col].isna().sum()
        null_pct = null_count / len(qry_df) * 100
        status = "✓" if null_pct == 0 else "✗"
        print(f"  {status} {col:<25} {null_count:>5} nulls ({null_pct:>5.1f}%)")
    
    # Check for duplicate customers
    excel_dups = excel_df[excel_df.duplicated(subset=['Customer Code'], keep=False)]
    qry_dups = qry_df[qry_df.duplicated(subset=['Sold-to Party'], keep=False)]
    
    print(f"\nDuplicate Detection:")
    print(f"  Excel duplicate customer codes: {len(excel_dups)}")
    print(f"  QRY duplicate customers: {len(qry_dups)}")

def match_customer_records(excel_df, qry_df):
    """Try to match records and find discrepancies"""
    print("\n" + "=" * 80)
    print("RECORD MATCHING ANALYSIS")
    print("=" * 80)
    
    # Convert customer codes to strings for matching
    excel_df['cust_key'] = excel_df['Customer Code'].astype(str).str.strip()
    qry_df['cust_key'] = qry_df['Sold-to Party'].astype(str).str.strip()
    
    # Find customers in both files
    excel_custs = set(excel_df['cust_key'].unique())
    qry_custs = set(qry_df['cust_key'].unique())
    matched_custs = excel_custs & qry_custs
    
    print(f"\nMatched Records: {len(matched_custs):,} customers in both files")
    
    # For matched customers, compare sales amounts
    print(f"\nSales Amount Validation (Sample of 20 matched customers):")
    print(f"  {'Customer Code':<15} {'Excel Sales':>15} {'QRY Sales':>15} {'Difference':>15}")
    print(f"  {'-'*60}")
    
    sample_count = 0
    for cust in sorted(matched_custs)[:20]:
        excel_sales = excel_df[excel_df['cust_key'] == cust]['MTD Sales Amount'].sum()
        qry_sales = qry_df[qry_df['cust_key'] == cust]['Total Value (EUR)'].sum()
        diff = excel_sales - qry_sales
        
        if sample_count < 20:
            print(f"  {cust:<15} €{excel_sales:>14,.2f} €{qry_sales:>14,.2f} €{diff:>14,.2f}")
            sample_count += 1
    
    # Customers only in Excel
    only_excel = excel_custs - qry_custs
    if only_excel:
        excel_only_sales = excel_df[excel_df['cust_key'].isin(only_excel)]['MTD Sales Amount'].sum()
        print(f"\nCustomers Only in Excel: {len(only_excel):,} customers")
        print(f"  Total Sales: €{excel_only_sales:,.2f}")
    
    # Customers only in QRY
    only_qry = qry_custs - excel_custs
    if only_qry:
        qry_only_sales = qry_df[qry_df['cust_key'].isin(only_qry)]['Total Value (EUR)'].sum()
        print(f"\nCustomers Only in QRY: {len(only_qry):,} customers")
        print(f"  Total Sales: €{qry_only_sales:,.2f}")

def generate_report(excel_df, qry_df, output_path=None):
    """Generate and optionally save detailed report"""
    report_lines = []
    
    report_lines.append("=" * 80)
    report_lines.append("DATA SOURCE COMPARISON REPORT")
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("=" * 80)
    
    # Summary
    excel_total = excel_df['MTD Sales Amount'].sum()
    qry_total = qry_df['Total Value (EUR)'].sum()
    
    report_lines.append("\nSUMMARY")
    report_lines.append(f"Excel Records: {len(excel_df):,}")
    report_lines.append(f"QRY Records: {len(qry_df):,}")
    report_lines.append(f"Excel Total Sales: €{excel_total:,.2f}")
    report_lines.append(f"QRY Total Sales: €{qry_total:,.2f}")
    report_lines.append(f"Difference: €{abs(excel_total - qry_total):,.2f} ({abs(excel_total - qry_total)/qry_total*100:.1f}%)")
    
    if output_path:
        try:
            with open(output_path, 'w') as f:
                f.write('\n'.join(report_lines))
            print(f"\n✓ Report saved to: {output_path}")
        except Exception as e:
            print(f"\n✗ Error saving report: {e}")
    else:
        print('\n'.join(report_lines))

def main():
    parser = argparse.ArgumentParser(
        description='Compare New vs Existing Customer Breakdown with QRY_AR_MTD_Gmbh.csv',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python compare_data_sources.py "20260128T133408.590-12-Month New vs. Existing Customer Breakdown.xlsx" "data/QRY_AR_MTD_Gmbh.csv"
  python compare_data_sources.py excel.xlsx qry.csv --verbose --output comparison_report.txt
  python compare_data_sources.py excel.xlsx qry.csv --summary-only
        '''
    )
    
    parser.add_argument('excel_file', help='Path to Excel file')
    parser.add_argument('qry_file', help='Path to QRY CSV file')
    parser.add_argument('--output', help='Save detailed report to file')
    parser.add_argument('--verbose', action='store_true', help='Show detailed analysis')
    parser.add_argument('--summary-only', action='store_true', help='Show only summary statistics')
    
    args = parser.parse_args()
    
    # Check files exist
    if not Path(args.excel_file).exists():
        print(f"✗ Excel file not found: {args.excel_file}")
        sys.exit(1)
    if not Path(args.qry_file).exists():
        print(f"✗ QRY file not found: {args.qry_file}")
        sys.exit(1)
    
    print("=" * 80)
    print("DATA SOURCE COMPARISON")
    print("=" * 80)
    print()
    
    # Load files
    excel_df = load_excel_file(args.excel_file)
    qry_df = load_qry_file(args.qry_file)
    
    # Run analyses
    if args.summary_only:
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        excel_total = excel_df['MTD Sales Amount'].sum()
        qry_total = qry_df['Total Value (EUR)'].sum()
        print(f"\nExcel: {len(excel_df):,} records, €{excel_total:,.2f} total sales")
        print(f"QRY:   {len(qry_df):,} records, €{qry_total:,.2f} total sales")
        print(f"Diff:  €{abs(excel_total - qry_total):,.2f} ({abs(excel_total - qry_total)/qry_total*100:.1f}%)")
    else:
        analyze_coverage(excel_df, qry_df)
        analyze_sales_by_employee(excel_df, qry_df)
        analyze_customer_status(excel_df)
        analyze_data_quality(excel_df, qry_df)
        
        if args.verbose:
            match_customer_records(excel_df, qry_df)
    
    # Generate report if requested
    if args.output:
        generate_report(excel_df, qry_df, args.output)
    
    print("\n✓ Comparison complete!")

if __name__ == '__main__':
    main()
