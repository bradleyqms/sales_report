import os
import pandas as pd
from pathlib import Path
from collections import defaultdict
import tempfile
import sys
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file in project root
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")

# Add current directory to path to import sharepoint_handler
sys.path.append(os.path.dirname(__file__))
try:
    from sharepoint_client import SharePointHandler
except ImportError:
    SharePointHandler = None

def process_qry_files_with_split(folder):
    """
    Reads QRY files from the specified folder and returns a unified DataFrame
    with sales employee split categorization (neukd vs non-neukd).
    """
    try:
        files = [f for f in os.listdir(folder) if "QRY" in f and f.endswith(".csv")]
    except FileNotFoundError:
        logging.error(f"Folder not found: {folder}")
        return pd.DataFrame()
    
    if not files:
        logging.warning(f"No QRY CSV files found in {folder}")
        return pd.DataFrame()
    all_data = []

    for file in files:
        path = os.path.join(folder, file)
        # Parse filename: QRY_[category]_[timeframe]_[region].csv
        parts = file.replace('QRY_', '').replace('.csv', '').split('_')
        if len(parts) >= 3:
            category = parts[0]
            if parts[1] in ['OPEN', 'TOTAL']:
                category += '_' + parts[1]
                timeframe = parts[2]
                region = '_'.join(parts[3:]) if len(parts) > 3 else ''
            else:
                timeframe = parts[1]
                region = '_'.join(parts[2:]) if len(parts) > 2 else ''
        else:
            category = timeframe = region = 'unknown'
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                # Strip trailing '=', then split on last '='
                line_stripped = line.rstrip('=')
                if '=' in line_stripped:
                    entity, value_str = line_stripped.rsplit('=', 1)
                    value_str = value_str.replace(',', '.')
                    try:
                        value = float(value_str)
                        all_data.append({
                            'entity': entity,
                            'value': value,
                            'category': category,
                            'timeframe': timeframe,
                            'region': region,
                            'file': file
                        })
                    except ValueError:
                        logging.warning(f"Could not parse value in {file}: {value_str} from {line}")
                else:
                    pass
        except Exception as e:
            logging.error(f"Error reading {file}: {e}")

    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    if df.empty:
        return pd.DataFrame()

    # Separate entity into sales_employee and customer based on region
    df['sales_employee'] = df.apply(lambda row: row['entity'] if row['region'].lower() in ['gmbh', 'ch'] else None, axis=1)
    df['customer'] = df.apply(lambda row: row['entity'] if row['region'].lower() not in ['gmbh', 'ch'] else None, axis=1)

    # Clean customer names: take the last part after '=' if present
    df['customer'] = df['customer'].apply(lambda x: x.split('=')[-1] if x and '=' in x else x)

    # Map region to Company Entity for compatibility with sales mapping
    region_to_entity = {'Gmbh': 'GmbH', 'GmbH': 'GmbH', 'CH': 'AG', 'Export': 'Export', 'USA': 'USA', 'UK': 'UK'}
    df['Company Entity'] = df['region'].map(region_to_entity).fillna(df['region'])

    # Map region to currency
    region_to_currency = {'Gmbh': 'EUR', 'GmbH': 'EUR', 'CH': 'CHF', 'Export': 'EUR', 'USA': 'USD', 'UK': 'GBP'}
    df['Currency'] = df['region'].map(region_to_currency).fillna('EUR')

    # ============ NEW: Add split categorization for GmbH and CH regions ============
    # Categorize sales employees as 'neukd' or 'regular'
    df['sales_employee_type'] = df.apply(
        lambda row: 'neukd' if row['sales_employee'] and 'neukd' in row['sales_employee'].lower() else 
                    'regular' if row['sales_employee'] else None,
        axis=1
    )
    # ============================================================================

    # Validate required columns
    required_cols = ['sales_employee', 'customer', 'value', 'category', 'Company Entity', 'Currency']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"Missing required columns: {missing_cols}")
        return pd.DataFrame()
    
    # Create formatted DataFrame for mapping compatibility
    qry_df = df[['sales_employee', 'sales_employee_type', 'customer', 'value', 'category', 'Company Entity', 'Currency', 'file']].copy()
    qry_df.rename(columns={
        'sales_employee': 'Sales Employee Name',
        'customer': 'Customer Name',
        'value': 'Total Value (EUR)',
        'category': 'Document Type',
        'file': 'Source_File'
    }, inplace=True)
    
    qry_df['Metric'] = 'Receivables'
    qry_df['Load_Timestamp'] = pd.Timestamp.now()
    qry_df['Value_in_EUR_converted'] = qry_df['Total Value (EUR)']  # Will be converted later if needed
    qry_df['Customer Code'] = None
    qry_df['Total Open Value (EUR)'] = qry_df['Total Value (EUR)']  # Assuming all are open for receivables

    # Apply FX conversion
    fx_rates = {"CHF": 1.08, "USD": 0.96, "GBP": 1.20, "EUR": 1.00}
    qry_df['Value_in_EUR_converted'] = qry_df.apply(
        lambda x: x['Total Value (EUR)'] * fx_rates.get(x['Currency'], 1), axis=1
    )
    
    return qry_df

def generate_sales_split_report(qry_df):
    """
    Generate a report showing sales breakdown by neukd vs regular employees
    for GmbH and CH regions, organized by document type.
    """
    if qry_df.empty:
        print("No data available for report generation.")
        return None
    
    # Filter for only GmbH and CH regions (which have sales_employee_type classification)
    split_data = qry_df[qry_df['sales_employee_type'].notna()].copy()
    
    if split_data.empty:
        print("No GmbH/CH sales data available.")
        return None
    
    print("\n" + "="*80)
    print("SALES SPLIT ANALYSIS REPORT - neukd vs Regular Sales Employees")
    print("="*80)
    
    # Overall summary by employee type
    print("\n1. OVERALL SUMMARY BY EMPLOYEE TYPE")
    print("-" * 80)
    overall = split_data.groupby('sales_employee_type')['Total Value (EUR)'].agg(['sum', 'count', 'mean']).round(2)
    overall.columns = ['Total Value (EUR)', 'Record Count', 'Avg Value (EUR)']
    print(overall)
    
    # Summary by company entity and employee type
    print("\n2. BREAKDOWN BY COMPANY ENTITY & EMPLOYEE TYPE")
    print("-" * 80)
    by_entity = split_data.groupby(['Company Entity', 'sales_employee_type'])['Total Value (EUR)'].agg(['sum', 'count']).round(2)
    by_entity.columns = ['Total Value (EUR)', 'Record Count']
    print(by_entity)
    
    # Summary by document type and employee type
    print("\n3. BREAKDOWN BY DOCUMENT TYPE & EMPLOYEE TYPE")
    print("-" * 80)
    by_doctype = split_data.groupby(['Document Type', 'sales_employee_type'])['Total Value (EUR)'].agg(['sum', 'count']).round(2)
    by_doctype.columns = ['Total Value (EUR)', 'Record Count']
    print(by_doctype)
    
    # Detailed breakdown: Company Entity + Document Type + Employee Type
    print("\n4. DETAILED BREAKDOWN (Company Entity + Document Type + Employee Type)")
    print("-" * 80)
    detailed = split_data.groupby(['Company Entity', 'Document Type', 'sales_employee_type'])['Total Value (EUR)'].agg(['sum', 'count']).round(2)
    detailed.columns = ['Total Value (EUR)', 'Record Count']
    print(detailed)
    
    # Top sales employees by type
    print("\n5. TOP 10 SALES EMPLOYEES BY TYPE")
    print("-" * 80)
    
    neukd_employees = split_data[split_data['sales_employee_type'] == 'neukd'].groupby('Sales Employee Name')['Total Value (EUR)'].sum().sort_values(ascending=False).head(10)
    print("\nTop neukd Employees:")
    print(neukd_employees.round(2))
    
    regular_employees = split_data[split_data['sales_employee_type'] == 'regular'].groupby('Sales Employee Name')['Total Value (EUR)'].sum().sort_values(ascending=False).head(10)
    print("\nTop Regular Employees:")
    print(regular_employees.round(2))
    
    # Create summary table for core_market_report integration
    print("\n6. SUMMARY TABLE FOR CORE MARKET REPORT INTEGRATION")
    print("-" * 80)
    summary_table = split_data.groupby(['Company Entity', 'Document Type']).apply(
        lambda x: pd.Series({
            'neukd_sales': x[x['sales_employee_type'] == 'neukd']['Total Value (EUR)'].sum(),
            'regular_sales': x[x['sales_employee_type'] == 'regular']['Total Value (EUR)'].sum(),
            'total_sales': x['Total Value (EUR)'].sum()
        })
    ).round(2)
    print(summary_table)
    
    print("\n" + "="*80)
    return summary_table

if __name__ == "__main__":
    # SharePoint configuration
    SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
    CLIENT_ID = os.getenv('SHAREPOINT_CLIENT_ID')
    CLIENT_SECRET = os.getenv('SHAREPOINT_CLIENT_SECRET')

    use_sharepoint = all([SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET]) and SharePointHandler

    temp_dir_obj = None

    if use_sharepoint:
        print("SharePoint credentials found. Downloading QRY files from SharePoint...")
        try:
            sp_handler = SharePointHandler(SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET)
            
            # Create a temporary directory
            temp_dir_obj = tempfile.TemporaryDirectory()
            folder = temp_dir_obj.name
            
            # List of files to download
            files_to_download = [
                "QRY_AR_MTD_CH.csv", "QRY_AR_MTD_Export.csv", "QRY_AR_MTD_Gmbh.csv", 
                "QRY_AR_MTD_UK.csv", "QRY_AR_MTD_USA.csv", 
                "QRY_CN_MTD_CH.csv", "QRY_CN_MTD_GmbH.csv", "QRY_CN_MTD_GmbH1.csv", 
                "QRY_CN_MTD_UK.csv", "QRY_CN_MTD_USA.csv", 
                "QRY_SO_OPEN_MTD_CH.csv", "QRY_SO_OPEN_MTD_Gmbh.csv", "QRY_SO_OPEN_MTD_USA.csv", 
                "QRY_SO_TOTAL_MTD_CH.csv", "QRY_SO_TOTAL_MTD_Gmbh.csv", "QRY_SO_TOTAL_MTD_USA.csv"
            ]
            
            # Base SharePoint path
            sp_base_path = "/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/"
            
            for filename in files_to_download:
                sp_path = sp_base_path + filename
                local_path = os.path.join(folder, filename)
                try:
                    sp_handler.download_file(sp_path, local_path)
                except Exception as e:
                    print(f"Warning: Could not download {filename}: {e}")
                    
        except Exception as e:
            print(f"Error connecting to SharePoint: {e}")
            print("Falling back to local automated_extracts folder.")
            folder = "automated_extracts"
    else:
        print("Using local automated_extracts folder.")
        folder = Path(__file__).parent.parent / "automated_extracts"

    # Process QRY files with split categorization
    qry_df = process_qry_files_with_split(folder)
    
    # Save full detailed data to outputs
    if not qry_df.empty:
        output_path = Path(__file__).parent.parent / "data/outputs/qry_sales_split_detailed.csv"
        qry_df.to_csv(output_path, index=False)
        print(f"\nDetailed QRY data with split categorization saved to {output_path}")
        
        # Generate and display the sales split report
        summary_table = generate_sales_split_report(qry_df)
        
        # Save summary table for integration with core_market_report
        if summary_table is not None:
            summary_path = Path(__file__).parent.parent / "data/outputs/sales_split_summary.csv"
            summary_table.to_csv(summary_path)
            print(f"\nSummary table saved to {summary_path}")
    else:
        print("No data processed!")
