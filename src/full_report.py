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

def print_progress(current, total, message=""):
    """Print a simple progress bar."""
    percentage = int((current / total) * 100)
    bar_length = 30
    filled_length = int(bar_length * current // total)
    bar = '#' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f'\r[{bar}] {percentage}% {message}')
    sys.stdout.flush()
    if current == total:
        print()  # New line when complete



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
            # Step 1: Download QRY files
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
            
            downloaded_count = 0
            original_stdout = sys.stdout
            sys.stdout = open(os.devnull, 'w')
            try:
                for filename in qry_files:
                    sp_path = sp_base_path + filename
                    local_path = os.path.join(temp_dir, filename)
                    try:
                        sp_handler.download_file(sp_path, local_path)
                        downloaded_count += 1
                    except Exception:
                        pass
            finally:
                sys.stdout = original_stdout
            
            print()
            print(f"[OK] Downloaded {downloaded_count}/{len(qry_files)} QRY files")
            
            # Step 2: Process QRY files
            print_progress(2, 6, "Processing QRY data...")
            qry_df = process_qry_files(temp_dir)
            print()
            print(f"[OK] Processed {len(qry_df)} QRY records")
            
            # Step 3: Download support files
            print_progress(3, 6, "Downloading support files...")
            
            other_paths = {
                'mapping': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/entity_mappings.csv',
                'budget': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_2025_processed.csv',
                'prior': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_2024_processed.csv'
            }
            
            local_paths = {}
            original_stdout = sys.stdout
            sys.stdout = open(os.devnull, 'w')
            try:
                for key, sp_path in other_paths.items():
                    local_path = os.path.join(temp_dir, os.path.basename(sp_path))
                    try:
                        sp_handler.download_file(sp_path, local_path)
                        local_paths[key] = local_path
                    except Exception:
                        # Fallback to local paths
                        if key == 'mapping':
                            local_paths[key] = str(project_root / 'data/inputs/mappings/entity_mappings.csv')
                        elif key == 'budget':
                            local_paths[key] = str(project_root / 'data/inputs/budget/budget_2025_processed.csv')
                        elif key == 'prior':
                            local_paths[key] = str(project_root / 'data/inputs/prior_years/prior_sales_2024_processed.csv')
            finally:
                sys.stdout = original_stdout
            
            print()
            print(f"[OK] Downloaded support files")
            
            # Step 4: Apply mappings
            print_progress(4, 6, "Applying entity mappings...")
            mapping_df = pd.read_csv(local_paths['mapping'])
            mapped_df = apply_mappings(qry_df, mapping_df)
            print()
            print(f"[OK] Mapped {len(mapped_df)} records")
            
            # Save unified mapped data
            mapped_path = os.path.join(temp_dir, 'qry_unified_mapped_2025.csv')
            mapped_df.to_csv(mapped_path, index=False)
            
            budget_path = local_paths['budget']
            prior_path = local_paths['prior']
            gvl_prior_path = str(project_root / 'data/inputs/prior_years/prior_sales_2024_gvl.csv')
            
        except Exception as e:
            logging.error(f"Error during data preparation: {e}")
            raise
            
    else:
        print("[INFO] Using local files for data sources")
        print()
        
        # Use existing local files
        mapped_path = str(project_root / 'data/outputs/qry_unified_mapped_2025.csv')
        budget_path = str(project_root / 'data/inputs/budget/budget_2025_processed.csv')
        prior_path = str(project_root / 'data/inputs/prior_years/prior_sales_2024_processed.csv')
        gvl_prior_path = str(project_root / 'data/inputs/prior_years/prior_sales_2024_gvl.csv')
        
        if not os.path.exists(mapped_path):
            logging.error(f"Mapped data file not found: {mapped_path}")
            logging.error("Please run data ingestion and mapping first, or configure SharePoint.")
            return
    
    # Create output directory
    output_dir = str(project_root / 'data/outputs')
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate timestamp for filenames
    now = datetime.datetime.now()
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    
    print()
    print("=" * 80)
    print("GENERATING REPORTS")
    print("=" * 80)
    print()
    
    # =========================================================================
    # REPORT 1: RECEIVABLES (MANAGEMENT) REPORT
    # =========================================================================
    print("-" * 80)
    print("REPORT 1: RECEIVABLES MANAGEMENT REPORT")
    print("-" * 80)
    print()
    
    try:
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
    # REPORT 2: GVL REPORT
    # =========================================================================
    print("-" * 80)
    print("REPORT 2: GVL REPORT (SALES BY EMPLOYEE)")
    print("-" * 80)
    print()
    
    try:
        # GVL report needs individual salesperson budgets, not aggregated
        gvl_budget_path = str(project_root / 'data/inputs/budget/budget_GVL_2025.csv')
        gvl_gen = GVLReportGenerator(
            str(project_root / 'src/config/gvl_report_structure.json'),
            mapped_path,
            gvl_budget_path,
            gvl_prior_path
        )
        gvl_df = gvl_gen.calculate_report()
        gvl_gen.render_report(gvl_df)
        
    except Exception as e:
        logging.error(f"Error generating GVL report: {e}")
        print(f"[ERROR] Failed to generate GVL report: {e}")
        print()
    
    # =========================================================================
    # REPORT 3: USA SPA REGIONAL REPORT
    # =========================================================================
    print("-" * 80)
    print("REPORT 3: USA SPA REGIONAL REPORT")
    print("-" * 80)
    print()
    
    try:
        usa_spa_gen = USASpaReportGenerator(
            str(project_root / 'src/config/usa_spa_report_structure.json'),
            mapped_path,
            budget_path,
            prior_path
        )
        usa_spa_df = usa_spa_gen.calculate_report()
        usa_spa_gen.render_report(usa_spa_df)
        
        # Rename columns to match other reports (actual -> sales)
        usa_spa_df = usa_spa_df.rename(columns={'actual': 'sales'})
        
    except Exception as e:
        logging.error(f"Error generating USA Spa report: {e}")
        print(f"[ERROR] Failed to generate USA Spa report: {e}")
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
    combined_base = os.path.join(output_dir, f'combined_management_report_2025_{timestamp}')
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