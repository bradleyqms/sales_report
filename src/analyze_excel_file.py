#!/usr/bin/env python
"""
Analyze New vs Existing Customer Breakdown Excel File

This script provides detailed analysis of the Excel file structure and data quality.
"""

import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(
        description='Analyze New vs Existing Customer Breakdown Excel file'
    )
    parser.add_argument('excel_file', help='Path to Excel file')
    parser.add_argument('--output', help='Save report to file')
    
    args = parser.parse_args()
    
    if not Path(args.excel_file).exists():
        print(f"✗ File not found: {args.excel_file}")
        return
    
    # Load file
    df = pd.read_excel(args.excel_file)
    
    print("=" * 80)
    print("EXCEL FILE ANALYSIS: New vs Existing Customer Breakdown")
    print("=" * 80)
    
    # Basic info
    print(f"\n📊 FILE INFORMATION")
    print(f"  File: {Path(args.excel_file).name}")
    print(f"  Records: {len(df):,}")
    print(f"  Columns: {', '.join(df.columns)}")
    print(f"  Data types: {dict(df.dtypes)}")
    
    # Data quality
    print(f"\n✓ DATA QUALITY")
    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct = null_count / len(df) * 100
        status = "✓" if null_count == 0 else "✗"
        print(f"  {status} {col:<25} {len(df) - null_count:>6} filled ({100 - null_pct:>5.1f}%)")
    
    # Sales Analysis
    print(f"\n💰 SALES ANALYSIS")
    total_sales = df['MTD Sales Amount'].sum()
    print(f"  Total MTD Sales: €{total_sales:,.2f}")
    print(f"  Average per customer: €{total_sales / len(df):,.2f}")
    print(f"  Min sales: €{df['MTD Sales Amount'].min():,.2f}")
    print(f"  Max sales: €{df['MTD Sales Amount'].max():,.2f}")
    print(f"  Median: €{df['MTD Sales Amount'].median():,.2f}")
    print(f"  Std Dev: €{df['MTD Sales Amount'].std():,.2f}")
    
    # Customer Status Analysis
    print(f"\n👥 CUSTOMER STATUS BREAKDOWN")
    status_analysis = df.groupby('Customer Status').agg({
        'Customer Code': 'count',
        'MTD Sales Amount': ['sum', 'mean']
    }).round(2)
    
    for status in df['Customer Status'].unique():
        status_df = df[df['Customer Status'] == status]
        count = len(status_df)
        sales = status_df['MTD Sales Amount'].sum()
        pct_customers = count / len(df) * 100
        pct_sales = sales / total_sales * 100
        avg = sales / count if count > 0 else 0
        print(f"\n  {status}:")
        print(f"    Count: {count:,} customers ({pct_customers:.1f}% of total)")
        print(f"    Sales: €{sales:,.2f} ({pct_sales:.1f}% of total)")
        print(f"    Avg per customer: €{avg:,.2f}")
    
    # Sales Employee Analysis
    print(f"\n👨‍💼 SALES EMPLOYEE BREAKDOWN")
    employee_analysis = df.groupby('Sales Employee')['MTD Sales Amount'].agg(['sum', 'count', 'mean']).sort_values('sum', ascending=False)
    
    print(f"\n  Top 10 Sales Employees:")
    for idx, (emp, row) in enumerate(employee_analysis.head(10).iterrows(), 1):
        pct = row['sum'] / total_sales * 100
        print(f"    {idx:2d}. {emp:<30} €{row['sum']:>12,.2f} ({pct:>5.1f}%)  {int(row['count']):>3} customers")
    
    # Bottom performers
    print(f"\n  Bottom 5 Sales Employees:")
    for idx, (emp, row) in enumerate(employee_analysis.tail(5).iloc[::-1].iterrows(), 1):
        pct = row['sum'] / total_sales * 100
        print(f"    {idx}. {emp:<30} €{row['sum']:>12,.2f} ({pct:>5.1f}%)  {int(row['count']):>3} customers")
    
    # Customer Analysis
    print(f"\n🏢 CUSTOMER CODE ANALYSIS")
    print(f"  Unique customers: {df['Customer Code'].nunique():,}")
    print(f"  Duplicates (customers with multiple records): {df.duplicated(subset=['Customer Code']).sum():,}")
    
    # Top customers
    print(f"\n  Top 10 Customers by Sales:")
    top_customers = df.nlargest(10, 'MTD Sales Amount')[['Sales Employee', 'Customer Code', 'Customer Name', 'Customer Status', 'MTD Sales Amount']]
    for idx, (_, row) in enumerate(top_customers.iterrows(), 1):
        pct = row['MTD Sales Amount'] / total_sales * 100
        print(f"    {idx:2d}. {row['Customer Code']:>6} - {row['Customer Name']:<40} €{row['MTD Sales Amount']:>10,.2f} ({pct:>4.1f}%)")
    
    # New vs Existing comparison
    print(f"\n📈 KEY INSIGHTS")
    new_customers = df[df['Customer Status'] == 'New']
    existing_customers = df[df['Customer Status'] == 'Existing']
    
    new_pct_count = len(new_customers) / len(df) * 100
    new_pct_sales = new_customers['MTD Sales Amount'].sum() / total_sales * 100
    
    print(f"  New customer concentration: {new_pct_count:.1f}% of customers, {new_pct_sales:.1f}% of sales")
    print(f"    → Ratio: {new_pct_sales / new_pct_count:.2f}x (higher = more valuable)")
    
    existing_pct_count = len(existing_customers) / len(df) * 100
    existing_pct_sales = existing_customers['MTD Sales Amount'].sum() / total_sales * 100
    
    print(f"  Existing customer concentration: {existing_pct_count:.1f}% of customers, {existing_pct_sales:.1f}% of sales")
    print(f"    → Ratio: {existing_pct_sales / existing_pct_count:.2f}x")
    
    # Concentration analysis
    print(f"\n  Sales Concentration:")
    top_10_pct = df.nlargest(10, 'MTD Sales Amount')['MTD Sales Amount'].sum() / total_sales * 100
    top_50_pct = df.nlargest(50, 'MTD Sales Amount')['MTD Sales Amount'].sum() / total_sales * 100
    print(f"    Top 10 customers: {top_10_pct:.1f}% of total sales")
    print(f"    Top 50 customers: {top_50_pct:.1f}% of total sales")
    
    # Employee concentration
    print(f"\n  Employee Concentration:")
    top_emp_sales = employee_analysis.iloc[0]['sum']
    top_5_emp_sales = employee_analysis.head(5)['sum'].sum()
    print(f"    Top employee: {top_emp_sales / total_sales * 100:.1f}% of total sales")
    print(f"    Top 5 employees: {top_5_emp_sales / total_sales * 100:.1f}% of total sales")
    
    # Comparison with QRY needs
    print(f"\n🔍 COMPARISON WITH QRY GVL DATA")
    print(f"  ✓ Has Sales Employee names - can map to employee breakdowns")
    print(f"  ✓ Has Customer Code - can match to QRY customer records")
    print(f"  ✓ Has sales amount - can compare to QRY total sales")
    print(f"  ✗ Missing Market Group - cannot segment by market")
    print(f"  ✗ Missing Region - cannot show geographic breakdown")
    print(f"  ✗ Missing Channel Level - cannot distinguish retail/spa/ecommerce")
    print(f"  ✗ Missing Document Type - cannot verify AR only")
    print(f"  ✓ New feature: Customer Status - can track new vs existing")
    
    print(f"\n📌 RECOMMENDATIONS")
    print(f"  1. Use this file for NEW CUSTOMER acquisition tracking")
    print(f"  2. Use for EMPLOYEE performance against new business targets")
    print(f"  3. SUPPLEMENT (not replace) QRY data with this breakdown")
    print(f"  4. Create dashboard showing new vs existing customer trends")
    print(f"  5. Track customer churn by comparing new customers each period")
    
    print("\n" + "=" * 80)

if __name__ == '__main__':
    main()
