import os
import pandas as pd
import numpy as np
import re
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
    # print("Could not import SharePointHandler. Ensure sharepoint_handler.py is in the same directory.")
    SharePointHandler = None

# Precompiled regex for parsing QRY lines: entity=value (with optional trailing =)
# Matches: "SalesEmployee=CustomerName=1234.56=" -> groups: ("SalesEmployee=CustomerName", "1234.56")
QRY_LINE_PATTERN = re.compile(r'^(.+?)=([0-9,.\-]+)=?$')

# FX rates as numpy-compatible dict
FX_RATES = {"CHF": 1.08, "USD": 0.96, "GBP": 1.20, "EUR": 1.00}

def parse_qry_file_batch(file_paths):
    """
    Parse multiple QRY files in batch using optimized string operations.
    Returns list of tuples: (entity, value, category, timeframe, region, filename)
    """
    all_data = []
    
    for path in file_paths:
        filename = os.path.basename(path)
        
        # Parse filename: QRY_[category]_[timeframe]_[region].csv
        parts = filename.replace('QRY_', '').replace('.csv', '').split('_')
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
            # Read entire file at once
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Use findall with compiled regex to extract all entity=value pairs
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                
                match = QRY_LINE_PATTERN.match(line)
                if match:
                    entity = match.group(1)
                    value_str = match.group(2).replace(',', '.')
                    try:
                        value = float(value_str)
                        all_data.append((entity, value, category, timeframe, region, filename))
                    except ValueError:
                        pass
        except Exception as e:
            logging.error(f"Error reading {filename}: {e}")
    
    return all_data

def process_qry_files(folder):
    """
    Reads QRY files from the specified folder and returns a unified DataFrame.
    Optimized with batch parsing and vectorized operations.
    """
    try:
        files = [os.path.join(folder, f) for f in os.listdir(folder) if "QRY" in f and f.endswith(".csv")]
    except FileNotFoundError:
        logging.error(f"Folder not found: {folder}")
        return pd.DataFrame()
    
    if not files:
        logging.warning(f"No QRY CSV files found in {folder}")
        return pd.DataFrame()
    
    # Batch parse all files
    all_data = parse_qry_file_batch(files)
    
    if not all_data:
        return pd.DataFrame()
    
    # Create DataFrame directly from tuples (more efficient than list of dicts)
    df = pd.DataFrame(all_data, columns=['entity', 'value', 'category', 'timeframe', 'region', 'file'])

    # Vectorized: Separate entity into sales_employee and customer based on region
    # Use np.where instead of apply(lambda)
    region_lower = df['region'].str.lower()
    is_gmbh_ch = region_lower.isin(['gmbh', 'ch'])
    
    df['sales_employee'] = np.where(is_gmbh_ch, df['entity'], None)
    df['customer'] = np.where(~is_gmbh_ch, df['entity'], None)

    # Vectorized: Clean customer names - extract last part after '=' if present
    # Use str.split with expand and take last column
    has_equals = df['customer'].notna() & df['customer'].str.contains('=', na=False)
    df.loc[has_equals, 'customer'] = df.loc[has_equals, 'customer'].str.rsplit('=', n=1).str[-1]

    # Vectorized: Map region to Company Entity
    region_to_entity = {'Gmbh': 'GmbH', 'GmbH': 'GmbH', 'CH': 'AG', 'Export': 'Export', 'USA': 'USA', 'UK': 'UK'}
    df['Company Entity'] = df['region'].map(region_to_entity).fillna(df['region'])

    # Vectorized: Map region to currency
    region_to_currency = {'Gmbh': 'EUR', 'GmbH': 'EUR', 'CH': 'CHF', 'Export': 'EUR', 'USA': 'USD', 'UK': 'GBP'}
    df['Currency'] = df['region'].map(region_to_currency).fillna('EUR')

    # Validate required columns
    required_cols = ['sales_employee', 'customer', 'value', 'category', 'Company Entity', 'Currency']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logging.error(f"Missing required columns: {missing_cols}")
        return pd.DataFrame()
    
    # Create formatted DataFrame for mapping compatibility
    qry_df = df[['sales_employee', 'customer', 'value', 'category', 'Company Entity', 'Currency', 'file']].copy()
    qry_df.columns = ['Sales Employee Name', 'Customer Name', 'Total Value (EUR)', 'Document Type', 
                      'Company Entity', 'Currency', 'Source_File']
    
    qry_df['Metric'] = 'Receivables'
    qry_df['Load_Timestamp'] = pd.Timestamp.now()
    qry_df['Customer Code'] = None
    qry_df['Total Open Value (EUR)'] = qry_df['Total Value (EUR)']

    # Vectorized FX conversion using map instead of apply
    fx_multiplier = qry_df['Currency'].map(FX_RATES).fillna(1.0)
    qry_df['Value_in_EUR_converted'] = qry_df['Total Value (EUR)'] * fx_multiplier

    return qry_df

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

    qry_df = process_qry_files(folder)
    
    # Save to outputs
    output_path = Path(__file__).parent.parent / "data/outputs/qry_unified_2025.csv"
    qry_df.to_csv(output_path, index=False)
    print(f"\nFormatted QRY data saved to {output_path}")
    
    # Print stats (simplified from original script)
    print(f"Total records: {len(qry_df)}")
    if not qry_df.empty:
        print("Sample data:")
        print(qry_df.head(5))
        print("\nValue summary by Document Type:")
        print(qry_df.groupby('Document Type')['Total Value (EUR)'].sum())
        print("\nSummary statistics:")
        print(qry_df['Total Value (EUR)'].describe())
    else:
        print("No data processed!")