import pandas as pd
import datetime
import os
import tempfile
import sys
import logging
import shutil
import warnings
import json
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress pandas FutureWarnings for concat and fillna
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Import the necessary modules
from sharepoint_client import SharePointHandler
from qry_data_ingestion import process_qry_files
from qry_data_mapping import apply_mappings
from receivables_report_generator import ManagementReportGenerator
from gvl_report import GVLReportGenerator
from usa_spa_report import USASpaReportGenerator
from core_market_report import CoreMarketReportGenerator
from utils import print_progress, get_current_year, get_prior_year, get_current_month, format_mtd_date_range



def main():
    start_time = datetime.datetime.now()
    
    # Load environment variables
    load_dotenv()

    # SharePoint configuration
    SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
    CLIENT_ID = os.getenv('SHAREPOINT_CLIENT_ID')
    CLIENT_SECRET = os.getenv('SHAREPOINT_CLIENT_SECRET')
    
    project_root = Path(__file__).parent.parent
    use_sharepoint = all([SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET])
    
    print("=" * 80)
    print("FULL MANAGEMENT REPORT GENERATION")
    print("=" * 80)
    print(f"Starting at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    current_year = get_current_year()  # Returns 2026
    prior_year = get_prior_year()      # Returns 2025
    
    # Update file paths to use dynamic year:
    budget_path = str(project_root / f'data/inputs/budget/budget_{current_year}_processed.csv')
    prior_path = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv')
    gvl_budget_path = str(project_root / f'data/inputs/budget/budget_GVL_{current_year}.csv')
    gvl_prior_path = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv')
    usa_spa_budget_path = str(project_root / f'data/inputs/budget/budget_USA_spa_{current_year}.csv')
    usa_spa_prior_path = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_usa.csv')
    
    # Determine if we use SharePoint or local files
    if use_sharepoint:
        print("[INFO] Using SharePoint for data sources")
        
        # Initialize SharePoint handler (suppress connection message)
        original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')
        try:
            sp_handler = SharePointHandler(SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET, quiet=True)
        finally:
            sys.stdout = original_stdout
        
        # Create temp directory for downloads
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Step 1: Download QRY files (PARALLEL)
            print_progress(1, 6, "Downloading QRY files from SharePoint...")
            
            qry_files = [
                "QRY_AR_MTD_CH.csv", "QRY_AR_MTD_Export.csv", "QRY_AR_MTD_Gmbh.csv", 
                "QRY_AR_MTD_UK.csv", "QRY_AR_MTD_USA.csv", 
                "QRY_CN_MTD_CH.csv", "QRY_CN_MTD_GmbH.csv", "QRY_CN_MTD_GmbH1.csv", 
                "QRY_CN_MTD_UK.csv", "QRY_CN_MTD_USA.csv", 
                "QRY_SO_OPEN_MTD_CH.csv", "QRY_SO_OPEN_MTD_Gmbh.csv", "QRY_SO_OPEN_MTD_USA.csv", 
                "QRY_SO_TOTAL_MTD_CH.csv", "QRY_SO_TOTAL_MTD_Gmbh.csv", "QRY_SO_TOTAL_MTD_USA.csv"
            ]
            
            sp_base_path = "/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/"
            
            def download_qry_file(filename):
                """Download a single QRY file. Returns (filename, success)."""
                sp_path = sp_base_path + filename
                local_path = os.path.join(temp_dir, filename)
                try:
                    sp_handler.download_file(sp_path, local_path)
                    return (filename, True)
                except Exception:
                    return (filename, False)
            
            # Use ThreadPoolExecutor for parallel downloads (6 workers to avoid rate limiting)
            downloaded_count = 0
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(download_qry_file, f): f for f in qry_files}
                for future in as_completed(futures):
                    filename, success = future.result()
                    if success:
                        downloaded_count += 1
            
            print()
            print(f"[OK] Downloaded {downloaded_count}/{len(qry_files)} QRY files")
            
            # Step 2: Process QRY files
            print_progress(2, 6, "Processing QRY data...")
            qry_df = process_qry_files(temp_dir)
            print()
            print(f"[OK] Processed {len(qry_df)} QRY records")
            
            # Step 3: Download support files (PARALLEL)
            print_progress(3, 6, "Downloading support files...")
            
            current_year = get_current_year()
            prior_year = get_prior_year()
            # Map of file type to SharePoint path
            other_paths = {
                'mapping': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/entity_mappings.csv',
                'budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_{current_year}_processed.csv',
                'prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_{prior_year}_processed.csv',
                'gvl_budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_GVL_{current_year}.csv',
                'gvl_prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_{prior_year}_gvl.csv',
                'usa_spa_budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_USA_spa_{current_year}.csv',
                'usa_spa_prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_{prior_year}_usa.csv'
            }
            
            # Local fallback paths
            fallback_paths = {
                'mapping': str(project_root / 'data/inputs/mappings/entity_mappings.csv'),
                'budget': str(project_root / f'data/inputs/budget/budget_{current_year}_processed.csv'),
                'prior': str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv'),
                'gvl_budget': str(project_root / f'data/inputs/budget/budget_GVL_{current_year}.csv'),
                'gvl_prior': str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv'),
                'usa_spa_budget': str(project_root / f'data/inputs/budget/budget_USA_spa_{current_year}.csv'),
                'usa_spa_prior': str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_usa.csv')
            }
            
            def download_support_file(key_sp_path):
                """Download a single support file. Returns (key, local_path, success)."""
                key, sp_path = key_sp_path
                local_path = os.path.join(temp_dir, os.path.basename(sp_path))
                try:
                    sp_handler.download_file(sp_path, local_path)
                    logging.info(f"[SP] Downloaded '{key}' to: {local_path}")
                    return (key, local_path, True)
                except Exception as e:
                    logging.warning(f"[SP] Failed to download '{key}': {e}. Using local fallback.")
                    return (key, fallback_paths[key], False)
            
            local_paths = {}
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(download_support_file, item): item[0] for item in other_paths.items()}
                for future in as_completed(futures):
                    key, local_path, success = future.result()
                    local_paths[key] = local_path
            
            print()
            print(f"[OK] Downloaded support files")
            
            # Step 4: Apply mappings
            print_progress(4, 6, "Applying entity mappings...")
            mapping_df = pd.read_csv(local_paths['mapping'])
            mapped_df = apply_mappings(qry_df, mapping_df)
            print()
            print(f"[OK] Mapped {len(mapped_df)} records")
            
            # Save unified mapped data
            current_year = get_current_year()
            prior_year = get_prior_year()
            mapped_path = os.path.join(temp_dir, f'qry_unified_mapped_{current_year}.csv')
            mapped_df.to_csv(mapped_path, index=False)
            
            budget_path = local_paths['budget']
            prior_path = local_paths['prior']
            gvl_prior_path = local_paths.get('gvl_prior', str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv'))
            logging.info(f"[SP] Resolved support file paths: budget={budget_path}, prior={prior_path}, gvl_budget={local_paths.get('gvl_budget')}, gvl_prior={gvl_prior_path}, usa_spa_budget={local_paths.get('usa_spa_budget')}, usa_spa_prior={local_paths.get('usa_spa_prior')}")
            
        except Exception as e:
            logging.error(f"Error during data preparation: {e}")
            raise
            
    else:
        print("[INFO] Using local files for data sources")
        print()
        
        # Use existing local files
        current_year = get_current_year()
        prior_year = get_prior_year()
        mapped_path = str(project_root / f'data/outputs/qry_unified_mapped_{current_year}.csv')
        budget_path = str(project_root / f'data/inputs/budget/budget_{current_year}_processed.csv')
        prior_path = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv')
        
        # Create local_paths dict for consistency
        local_paths = {
            'budget': budget_path,
            'prior': prior_path,
            'gvl_budget': str(project_root / f'data/inputs/budget/budget_GVL_{current_year}.csv'),
            'gvl_prior': str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv'),
            'usa_spa_budget': str(project_root / f'data/inputs/budget/budget_USA_spa_{current_year}.csv'),
            'usa_spa_prior': str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_usa.csv')
        }
        
        if not os.path.exists(mapped_path):
            logging.error(f"Mapped data file not found: {mapped_path}")
            logging.error("Please run data ingestion and mapping first, or configure SharePoint.")
            return
    
    # Create output directory — REPORT_OUTPUT_DIR lets Azure redirect to /tmp/outputs
    output_dir = os.environ.get('REPORT_OUTPUT_DIR', str(project_root / 'data/outputs'))
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate timestamp for filenames
    now = datetime.datetime.now()
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    
    print()
    print("=" * 80)
    print("GENERATING REPORTS (MONTH-TO-DATE)")
    print("=" * 80)
    print()
    
    # =========================================================================
    # REPORT 1: RECEIVABLES (MANAGEMENT) REPORT
    # =========================================================================
    print("-" * 80)
    print("REPORT 1: RECEIVABLES MANAGEMENT REPORT (MTD)")
    print("-" * 80)
    print()
    
    try:
        logging.info(f"[REPORT] Receivables using budget_path={budget_path}, prior_path={prior_path}")
        receivables_gen = ManagementReportGenerator(
            str(project_root / 'src/config/report_structure.json'),
            mapped_path,
            budget_path,
            prior_path
        )
        receivables_df = receivables_gen.calculate_report()
        receivables_gen.render_report(receivables_df)
        
    except Exception as e:
        logging.error(f"Error generating Receivables report: {e}")
        print(f"[ERROR] Failed to generate Receivables report: {e}")
        print()
    
    # =========================================================================
    # REPORT 2: USA SPA REGIONAL REPORT
    # =========================================================================
    print("-" * 80)
    print("REPORT 2: USA SPA REGIONAL REPORT (MTD)")
    print("-" * 80)
    print()
    
    try:
        usa_spa_budget_path = local_paths.get('usa_spa_budget', str(project_root / f'data/inputs/budget/budget_USA_spa_{get_current_year()}.csv'))
        usa_spa_prior_path = local_paths.get('usa_spa_prior', str(project_root / f'data/inputs/prior_years/prior_sales_{get_prior_year()}_usa.csv'))
        logging.info(f"[REPORT] USA SPA using budget_path={usa_spa_budget_path}, prior_path={usa_spa_prior_path}")
        usa_spa_gen = USASpaReportGenerator(
            str(project_root / 'src/config/usa_spa_report_structure.json'),
            mapped_path,
            usa_spa_budget_path,
            usa_spa_prior_path
        )
        usa_spa_df = usa_spa_gen.calculate_report()
        usa_spa_gen.render_report(usa_spa_df)
        
        # Rename columns to match other reports (actual -> sales)
        usa_spa_df = usa_spa_df.rename(columns={'actual': 'sales'})
        
        # Do not scale USA Spa budget; values are already in k-units per report
        
    except Exception as e:
        logging.error(f"Error generating USA Spa report: {e}")
        print(f"[ERROR] Failed to generate USA Spa report: {e}")
        print()
    
    # =========================================================================
    # REPORT 3: CORE MARKET REPORT
    # =========================================================================
    print("-" * 80)
    print("REPORT 3: CORE MARKET REPORT (SUB-REGION BREAKDOWN) - MTD")
    print("-" * 80)
    print()
    
    try:
        # Use GVL budget for Core Market Report (has sub-region breakdown)
        core_market_budget_path = local_paths.get('gvl_budget', str(project_root / f'data/inputs/budget/budget_GVL_{get_current_year()}.csv'))
        core_market_prior_path = local_paths.get('gvl_prior', str(project_root / f'data/inputs/prior_years/prior_sales_{get_prior_year()}_gvl.csv'))
        logging.info(f"[REPORT] Core Market using budget_path={core_market_budget_path}, prior_path={core_market_prior_path}")
        
        core_market_gen = CoreMarketReportGenerator(
            str(project_root / 'src/config/core_market_report_structure.json'),
            mapped_path,
            core_market_budget_path,
            core_market_prior_path
        )
        core_market_df = core_market_gen.calculate_report()
        core_market_gen.render_report(core_market_df)
        
        # Export Core Market Report to separate CSV
        core_market_base = os.path.join(output_dir, f'management_report_core_markets_{get_current_year()}_{timestamp}')
        core_market_gen.export_report(core_market_df, core_market_base + '.csv')
        
    except Exception as e:
        logging.error(f"Error generating Core Market report: {e}")
        print(f"[ERROR] Failed to generate Core Market report: {e}")
        print()
    
    # =========================================================================
    # COMBINE REPORTS AND EXPORT
    # =========================================================================
    print("-" * 80)
    print("COMBINING REPORTS INTO SINGLE OUTPUT")
    print("-" * 80)
    print()
    
    # Clean up only prior combined report files from static
    static_dir = project_root / 'fastapi_web_app' / 'static'
    static_dir.mkdir(parents=True, exist_ok=True)
    for pattern in [
        'combined_management_report_*.csv',
        'combined_management_report_*.html',
        'combined_management_report_*.pdf',
        'combined_management_report_*.txt',
        'combined_management_report_*.xlsx',
        'combined_reports_*.zip'
    ]:
        for old_file in static_dir.glob(pattern):
            try:
                old_file.unlink()
            except OSError:
                logging.warning(f"Unable to remove {old_file}")
    
    # Create separator rows with consistent schema and proper types
    separator_receivables = pd.DataFrame([{
        'label': '=== RECEIVABLES MANAGEMENT REPORT ===',
        'sales': 0.0, 'budget': 0.0, 'prior': 0.0,
        'is_spacer': True, 'is_total': False, 'is_grand_total': False
    }])
    separator_gvl = pd.DataFrame([{
        'label': '=== GVL REPORT (SALES BY EMPLOYEE) ===',
        'sales': 0.0, 'budget': 0.0, 'prior': 0.0,
        'is_spacer': True, 'is_total': False, 'is_grand_total': False
    }])
    separator_usa_spa = pd.DataFrame([{
        'label': '=== USA SPA REGIONAL REPORT ===',
        'sales': 0.0, 'budget': 0.0, 'prior': 0.0,
        'is_spacer': True, 'is_total': False, 'is_grand_total': False
    }])
    
    # Combine DataFrames - filter out empty ones to avoid concat warning
    dfs_to_combine = []
    
    # Add receivables report
    if 'receivables_df' in locals() and not receivables_df.empty:
        dfs_to_combine.append(separator_receivables)
        dfs_to_combine.append(receivables_df)
    
    # Add GVL report
    if 'gvl_df' in locals() and not gvl_df.empty:
        dfs_to_combine.append(separator_gvl)
        dfs_to_combine.append(gvl_df)
    
    # Add USA Spa report
    if 'usa_spa_df' in locals() and not usa_spa_df.empty:
        dfs_to_combine.append(separator_usa_spa)
        dfs_to_combine.append(usa_spa_df)
    
    # Only concatenate if we have data
    if not dfs_to_combine:
        logging.error("No report data generated")
        sys.exit(1)
    
    combined_df = pd.concat(dfs_to_combine, ignore_index=True)
    
    # Export combined report
    current_year = get_current_year()
    combined_base = os.path.join(output_dir, f'combined_management_report_{current_year}_{timestamp}')
    receivables_gen.export_report(combined_df, combined_base + '.csv')
    print()
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print()
    print("=" * 80)
    print("REPORT GENERATION COMPLETE")
    print("=" * 80)
    print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total runtime: {duration:.2f} seconds")
    print()
    print(f"Combined report saved to: {output_dir}/")
    print(f"Timestamp: {timestamp}")
    print()
    print("Generated combined report with all sections:")
    print(f"  1. Receivables Management Report")
    print(f"  2. GVL Report (Sales by Employee)")
    print(f"  3. USA Spa Regional Report")
    print()
    print("Exported in 4 formats: CSV, TXT, HTML, PDF")
    print("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[CANCELLED] Report generation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n[ERROR] Fatal error occurred: {e}")
        sys.exit(1)