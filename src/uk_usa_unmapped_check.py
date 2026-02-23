#!/usr/bin/env python3
"""
UK and USA Input Files Unmapped Entities Check

This script checks UK and USA specific input files for unmapped entities
that cannot be properly categorized in the management reports.

Features:
- Downloads latest QRY files from SharePoint if credentials are available
- Falls back to local automated_extracts directory if SharePoint unavailable
- Supports timestamped files from automated extract processing
- Generates detailed reports of unmapped customer entities

SharePoint Integration:
- Requires SHAREPOINT_SITE_URL, SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET env vars
- Downloads from: /sites/DATAANDREPORTING/Shared Documents/SAP Extracts/
- Processes all QRY_AR, QRY_CN, and QRY_SO files for UK and USA
"""

import pandas as pd
import os
import datetime
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load .env from project root (sales_report_v2_independent/)
_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / '.env')

# Ensure src/ is on sys.path so 'from sharepoint_client import ...' works
_SRC_DIR = str(Path(__file__).parent)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

def get_current_year():
    """Get current year."""
    return datetime.datetime.now().year

def get_prior_year():
    """Get prior year."""
    return get_current_year() - 1

def load_entity_mappings(project_root: Path) -> pd.DataFrame:
    """Load entity mappings from the standard location."""
    mapping_paths = [
        project_root / 'data/inputs/mappings/entity_mappings.csv',
        project_root.parent / 'inputs_backup/entity_mappings.csv'
    ]

    for mapping_path in mapping_paths:
        if mapping_path.exists():
            try:
                df = pd.read_csv(mapping_path)
                logging.info(f"Loaded entity mappings from {mapping_path}")
                return df
            except Exception as e:
                logging.warning(f"Failed to load {mapping_path}: {e}")
                continue

    raise FileNotFoundError("Could not find entity_mappings.csv in expected locations")

def find_uk_usa_input_files(project_root: Path) -> Dict[str, List[Path]]:
    """Find UK and USA specific SALES input files (QRY files) from SharePoint."""
    files_found = {
        'usa_sales': [],
        'uk_sales': []
    }

    # Try to download from SharePoint first
    SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
    CLIENT_ID = os.getenv('SHAREPOINT_CLIENT_ID')
    CLIENT_SECRET = os.getenv('SHAREPOINT_CLIENT_SECRET')

    use_sharepoint = all([SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET])

    if use_sharepoint:
        try:
            from sharepoint_client import SharePointHandler
            import tempfile
            import sys

            logging.info("Attempting to download latest QRY files from SharePoint...")

            # Initialize SharePoint handler (quiet mode)
            sp_handler = SharePointHandler(SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET, quiet=True)

            # Create temp directory for downloads
            temp_dir = tempfile.mkdtemp()

            # List of QRY files to download
            qry_files = [
                "QRY_AR_MTD_CH.csv", "QRY_AR_MTD_Export.csv", "QRY_AR_MTD_Gmbh.csv",
                "QRY_AR_MTD_UK.csv", "QRY_AR_MTD_USA.csv",
                "QRY_CN_MTD_CH.csv", "QRY_CN_MTD_GmbH.csv", "QRY_CN_MTD_GmbH1.csv",
                "QRY_CN_MTD_UK.csv", "QRY_CN_MTD_USA.csv",
                "QRY_SO_OPEN_MTD_CH.csv", "QRY_SO_OPEN_MTD_Gmbh.csv", "QRY_SO_OPEN_MTD_USA.csv",
                "QRY_SO_TOTAL_MTD_CH.csv", "QRY_SO_TOTAL_MTD_Gmbh.csv", "QRY_SO_TOTAL_MTD_USA.csv"
            ]

            sp_base_path = "/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/"

            # Download QRY files (suppress individual prints)
            downloaded_count = 0
            original_stdout = sys.stdout
            sys.stdout = open(os.devnull, 'w')  # Suppress prints during downloads
            try:
                for filename in qry_files:
                    sp_path = sp_base_path + filename
                    local_path = os.path.join(temp_dir, filename)
                    try:
                        sp_handler.download_file(sp_path, local_path)
                        downloaded_count += 1
                    except Exception as e:
                        logging.warning(f"Failed to download {filename}: {e}")
                        continue
            finally:
                sys.stdout = original_stdout  # Restore stdout

            logging.info(f"Successfully downloaded {downloaded_count} QRY files from SharePoint to {temp_dir}")

            # Find UK and USA files in downloaded files
            temp_path = Path(temp_dir)
            for file_path in temp_path.glob('*.csv'):
                if any(keyword in file_path.name.upper() for keyword in ['QRY', 'AR_', 'CN_', 'SO_']):
                    if 'UK' in file_path.name.upper():
                        files_found['uk_sales'].append(file_path)
                    elif 'USA' in file_path.name.upper():
                        files_found['usa_sales'].append(file_path)

            return files_found

        except Exception as e:
            logging.warning(f"SharePoint download failed: {e}. Falling back to local files.")

    # Fallback to local automated_extracts directory
    logging.info("Using local automated_extracts directory")
    sales_report_v2_dir = project_root.parent / 'sales_report_v2' / 'automated_extracts'

    if sales_report_v2_dir.exists():
        # Find UK QRY files (including timestamped ones)
        for file_path in sales_report_v2_dir.glob('*UK*.csv'):
            if any(keyword in file_path.name.upper() for keyword in ['QRY', 'AR_', 'CN_', 'SO_']):
                files_found['uk_sales'].append(file_path)

        # Find USA QRY files (including timestamped ones)
        for file_path in sales_report_v2_dir.glob('*USA*.csv'):
            if any(keyword in file_path.name.upper() for keyword in ['QRY', 'AR_', 'CN_', 'SO_']):
                files_found['usa_sales'].append(file_path)

    return files_found

def check_unmapped_entities_in_file(
    file_path: Path,
    mapping_df: pd.DataFrame,
    region: str
) -> pd.DataFrame:
    """
    Check a sales input file for unmapped customer entities.

    Args:
        file_path: Path to the sales input file (QRY file)
        mapping_df: Entity mappings DataFrame
        region: 'UK' or 'USA' for context

    Returns:
        DataFrame with unmapped customer entities found
    """
    try:
        # Load the QRY file (custom format varies by region)
        customers = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                # Parse QRY format: varies by region and file type
                parts = line.split('=')
                if len(parts) >= 2:
                    if 'USA' in file_path.name.upper() and 'AR_' in file_path.name.upper():
                        # USA AR format: CustomerCode=CustomerName=Amount,CurrencyCode=
                        if len(parts) >= 3:
                            customer_name = parts[1].strip()
                        else:
                            continue
                    else:
                        # UK files and USA non-AR files: CustomerName=Amount,CurrencyCode=
                        customer_name = parts[0].strip()
                    
                    if customer_name:  # Only add non-empty customer names
                        customers.append(customer_name)
        
        if not customers:
            logging.info(f"Checking {file_path.name} (0 rows)")
            return pd.DataFrame()
        
        logging.info(f"Checking {file_path.name} ({len(customers)} customers)")

        unmapped_records = []
        entity_counts = {}

        # Count occurrences of each customer
        for customer in customers:
            entity_counts[customer] = entity_counts.get(customer, 0) + 1

        # Check each unique entity against mappings
        for entity_name, count in entity_counts.items():
            entity_str = str(entity_name).strip()

            # Skip empty/null values
            if not entity_str or entity_str.lower() in ['nan', 'none', '']:
                continue

            # Check if entity exists in mappings
            entity_mapped = False

            # Check Customer_Name column in mappings
            if 'Customer_Name' in mapping_df.columns:
                if entity_str in mapping_df['Customer_Name'].dropna().str.strip().values:
                    entity_mapped = True

            # Check Sales_Employee column for employee mappings
            if not entity_mapped and 'Sales_Employee' in mapping_df.columns:
                if entity_str in mapping_df['Sales_Employee'].dropna().str.strip().values:
                    entity_mapped = True

            # If not mapped, record it
            if not entity_mapped:
                unmapped_records.append({
                    'region': region,
                    'file_name': file_path.name,
                    'entity_type': 'customer',
                    'entity_name': entity_str,
                    'count': count,
                    'additional_info': ''
                })

        return pd.DataFrame(unmapped_records)

    except Exception as e:
        logging.error(f"Error checking {file_path}: {e}")
        return pd.DataFrame()

def check_budget_prior_mappings(df: pd.DataFrame, file_path: Path, region: str) -> pd.DataFrame:
    """
    Check budget/prior files for unmapped regions, channels, and company groups.

    Args:
        df: DataFrame from budget/prior file
        file_path: Path to the file
        region: 'UK' or 'USA'

    Returns:
        DataFrame with unmapped items
    """
    unmapped_records = []

    # Define expected values for validation
    expected_company_groups = ['Company 1', 'Company 2', 'Company 3']
    expected_market_groups = ['USA', 'UK', 'Core Markets', 'Export']

    # For USA files, check regions
    if region == 'USA':
        usa_regions = [
            'Northeast', 'Midwest', 'South', 'Southwest', 'West',
            'Spa', 'Retail', 'eCommerce EU (incl. UK)', 'Amazon',
            'eCommerce USA', 'Global eTailers', 'Distributor - Austria',
            'Distributor - South Africa', 'Distributor - Russia',
            'Distributor - Other EU', 'Distributor - Other ROW',
            'Distributor - New', 'Export - Direct business', 'Other Export'
        ]

        if 'Region' in df.columns:
            unique_regions = df['Region'].dropna().unique()
            for reg in unique_regions:
                reg_str = str(reg).strip()
                if reg_str and reg_str not in usa_regions:
                    count = len(df[df['Region'] == reg])
                    unmapped_records.append({
                        'region': region,
                        'file_name': file_path.name,
                        'entity_type': 'region',
                        'entity_name': reg_str,
                        'count': count,
                        'additional_info': f'Not in expected {region} regions list'
                    })

    # For UK files, check regions
    elif region == 'UK':
        uk_regions = ['Spa', 'Retail', 'Other']

        if 'Region' in df.columns:
            unique_regions = df['Region'].dropna().unique()
            for reg in unique_regions:
                reg_str = str(reg).strip()
                if reg_str and reg_str not in uk_regions:
                    count = len(df[df['Region'] == reg])
                    unmapped_records.append({
                        'region': region,
                        'file_name': file_path.name,
                        'entity_type': 'region',
                        'entity_name': reg_str,
                        'count': count,
                        'additional_info': f'Not in expected {region} regions list'
                    })

    # Check Company_Group
    if 'Company_Group' in df.columns:
        unique_companies = df['Company_Group'].dropna().unique()
        for comp in unique_companies:
            comp_str = str(comp).strip()
            if comp_str and comp_str not in expected_company_groups:
                count = len(df[df['Company_Group'] == comp])
                unmapped_records.append({
                    'region': region,
                    'file_name': file_path.name,
                    'entity_type': 'company_group',
                    'entity_name': comp_str,
                    'count': count,
                    'additional_info': f'Not in expected company groups: {expected_company_groups}'
                })

    # Check Market_Group
    if 'Market_Group' in df.columns:
        unique_markets = df['Market_Group'].dropna().unique()
        for market in unique_markets:
            market_str = str(market).strip()
            if market_str and market_str not in expected_market_groups:
                count = len(df[df['Market_Group'] == market])
                unmapped_records.append({
                    'region': region,
                    'file_name': file_path.name,
                    'entity_type': 'market_group',
                    'entity_name': market_str,
                    'count': count,
                    'additional_info': f'Not in expected market groups: {expected_market_groups}'
                })

    # Check Channel_Level for known values
    if 'Channel_Level' in df.columns:
        expected_channels = [
            'Spa', 'Retail', 'eCommerce EU (incl. UK)', 'Amazon',
            'eCommerce USA', 'Global eTailers', 'Distributor - Austria',
            'Distributor - South Africa', 'Distributor - Russia',
            'Distributor - Other EU', 'Distributor - Other ROW',
            'Distributor - New', 'Export - Direct business'
        ]

        unique_channels = df['Channel_Level'].dropna().unique()
        for ch in unique_channels:
            ch_str = str(ch).strip()
            if ch_str and ch_str not in expected_channels:
                count = len(df[df['Channel_Level'] == ch])
                unmapped_records.append({
                    'region': region,
                    'file_name': file_path.name,
                    'entity_type': 'channel',
                    'entity_name': ch_str,
                    'count': count,
                    'additional_info': f'Not in expected channels list'
                })

    return pd.DataFrame(unmapped_records)

def generate_unmapped_report(project_root: Path) -> str:
    """Generate a comprehensive report of unmapped entities in UK/USA input files."""

    logging.info("Starting UK/USA unmapped entities check...")

    # Load entity mappings
    try:
        mapping_df = load_entity_mappings(project_root)
    except FileNotFoundError as e:
        return f"ERROR: {e}"

    # Find input files
    input_files = find_uk_usa_input_files(project_root)

    # Check each file for unmapped entities
    all_unmapped = []

    for file_type, file_list in input_files.items():
        region = 'USA' if 'usa' in file_type else 'UK'

        for file_path in file_list:
            logging.info(f"Checking {file_type}: {file_path.name}")
            unmapped_df = check_unmapped_entities_in_file(file_path, mapping_df, region)
            if not unmapped_df.empty:
                all_unmapped.append(unmapped_df)

    # Combine all results
    if all_unmapped:
        final_report = pd.concat(all_unmapped, ignore_index=True)
    else:
        final_report = pd.DataFrame(columns=['region', 'file_name', 'entity_type', 'entity_name', 'count', 'additional_info'])

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = project_root / 'data' / 'outputs'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save detailed report
    csv_path = output_dir / f'uk_usa_sales_unmapped_entities_check_{timestamp}.csv'
    final_report.to_csv(csv_path, index=False)

    # Generate summary report
    summary_lines = []
    summary_lines.append("=" * 80)
    summary_lines.append("UK & USA SALES INPUT FILES UNMAPPED ENTITIES CHECK")
    summary_lines.append("=" * 80)
    summary_lines.append(f"Report generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary_lines.append("")

    # Files checked
    summary_lines.append("SALES FILES CHECKED:")
    for file_type, file_list in input_files.items():
        region = 'USA' if 'usa' in file_type else 'UK'
        summary_lines.append(f"  {region} {file_type.replace('_', ' ').title()}: {len(file_list)} files")
        for file_path in file_list:
            summary_lines.append(f"    - {file_path.name}")
    summary_lines.append("")

    # Results
    if final_report.empty:
        summary_lines.append("✅ RESULT: No unmapped entities found!")
        summary_lines.append("All customer entities in UK and USA sales input files are properly mapped.")
    else:
        summary_lines.append("❌ RESULT: Unmapped entities found!")
        summary_lines.append(f"Total unmapped entities: {len(final_report)}")

        # Summary by region and type
        region_summary = final_report.groupby(['region', 'entity_type']).size().unstack(fill_value=0)
        total_by_region = final_report.groupby('region').size()

        summary_lines.append("")
        summary_lines.append("SUMMARY BY REGION:")
        for region in ['UK', 'USA']:
            if region in total_by_region:
                summary_lines.append(f"  {region}: {total_by_region[region]} unmapped entities")
                if region in region_summary.index:
                    type_counts = region_summary.loc[region]
                    for entity_type, count in type_counts.items():
                        if count > 0:
                            summary_lines.append(f"    {entity_type}: {count}")

        # Top unmapped entities
        summary_lines.append("")
        summary_lines.append("TOP 10 UNMAPPED ENTITIES:")
        top_unmapped = final_report.nlargest(10, 'count')
        for _, row in top_unmapped.iterrows():
            summary_lines.append(f"  {row['region']} | {row['entity_type']} | {row['entity_name']} | {int(row['count'])} occurrences | File: {row['file_name']}")

    summary_lines.append("")
    summary_lines.append(f"Detailed report saved to: {csv_path}")
    summary_lines.append("=" * 80)

    # Save summary report
    txt_path = output_dir / f'uk_usa_sales_unmapped_entities_check_{timestamp}.txt'
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary_lines))

    # Print summary to console
    print('\n'.join(summary_lines))

    return f"Report completed. Files saved to {output_dir}"

if __name__ == "__main__":
    # Get project root
    project_root = Path(__file__).parent.parent

    try:
        result = generate_unmapped_report(project_root)
        print(f"\n{result}")
    except Exception as e:
        logging.error(f"Error generating report: {e}")
        print(f"ERROR: {e}")