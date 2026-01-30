import pandas as pd
import json
import datetime
import os
import tempfile
import sys
import time
import logging
from pathlib import Path
from typing import List
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from sharepoint_client import SharePointHandler, download_inputs, upload_outputs
from qry_data_ingestion import process_qry_files
from qry_data_mapping import apply_mappings
from utils import print_progress, get_current_year, get_prior_year, get_current_month, format_mtd_date_range
from base_report_generator import BaseReportGenerator

class GVLReportGenerator(BaseReportGenerator):
    """
    GVL Report Generator for sales by employee.
    
    Generates reports showing sales by individual sales employee
    with budget and prior year comparisons.
    """
    
    def __init__(self, config_path, sales_path, budget_path, prior_path):
        # Call parent constructor (loads config, data files, prepares dates)
        super().__init__(config_path, sales_path, budget_path, prior_path)
        self._prepare_data()
    
    def get_report_headers(self) -> List[str]:
        """Return column headers for the GVL report."""
        now = datetime.datetime.now()
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        return ['kEUR', f'{month_name}-{year_short}A MTD', 'Budget', 'Prior', '% vs Bud']
    
    def get_report_title(self) -> str:
        """Return the report title."""
        return "GVL Management Report"
    
    def format_row_for_export(self, row: pd.Series) -> List[str]:
        """Format a row for export to CSV/TXT/HTML/PDF."""
        label = row['label']
        sales = row['sales']
        budget = row['budget']
        prior = row['prior']
        
        pct = (sales / budget * 100) if budget and budget != 0 else 0
        
        s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
        b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
        p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
        pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
        
        return [label, s_str, b_str, p_str, pct_str]
            
    def _prepare_data(self):
        # Filter Sales to AR (for QRY data, Document Type is 'AR', not 'AR Invoice')
        self.df = self.df[self.df['Document Type'] == 'AR'].copy()
        
        # Convert Sales to kEUR
        value_col = 'Value_in_EUR_converted' if 'Value_in_EUR_converted' in self.df.columns else 'Total Value (EUR)'
        self.df['kEUR'] = self.df[value_col].fillna(0) / 1000
        
        # Clean Sales Employee in budget and prior
        self.budget_df['Sales_Employee_Cleaned'] = self.budget_df['Sales Employee / Account'].fillna('').str.strip()
        self.prior_df['Sales_Employee_Cleaned'] = self.prior_df['Sales Employee / Account'].fillna('').str.strip()
        
        logging.info(f"Budget DF loaded: {len(self.budget_df)} rows, columns: {self.budget_df.columns.tolist()}")

        # Map Sales Employees to Region using entity mappings (for roll-up alignment)
        repo_root = Path(__file__).parent.parent
        mapping_path = repo_root / 'data/inputs/mappings/entity_mappings.csv'
        self.employee_region_map = {}
        if mapping_path.exists():
            try:
                mapping_df = pd.read_csv(mapping_path)
                mapping_df['Sales_Employee'] = mapping_df['Sales_Employee'].fillna('').str.strip()
                mapping_df['Sales_Employee_Cleaned'] = mapping_df['Sales_Employee_Cleaned'].fillna('').str.strip()
                mapping_df['Region'] = mapping_df['Region'].fillna('').str.strip()

                cleaned_map = mapping_df[mapping_df['Sales_Employee_Cleaned'] != ''].drop_duplicates(subset=['Sales_Employee_Cleaned'])
                raw_map = mapping_df[mapping_df['Sales_Employee'] != ''].drop_duplicates(subset=['Sales_Employee'])

                self.employee_region_map.update(dict(zip(cleaned_map['Sales_Employee_Cleaned'], cleaned_map['Region'])))
                self.employee_region_map.update({
                    k: v for k, v in dict(zip(raw_map['Sales_Employee'], raw_map['Region'])).items()
                    if k not in self.employee_region_map
                })

                self.budget_df['Region'] = self.budget_df['Sales_Employee_Cleaned'].map(self.employee_region_map).fillna('')
                self.prior_df['Region'] = self.prior_df['Sales_Employee_Cleaned'].map(self.employee_region_map).fillna('')
            except Exception as e:
                logging.warning(f"Could not apply employee-region mapping: {e}")
        else:
            logging.warning(f"Mapping file not found: {mapping_path}")
        
        # Filter Budget for Current Month
        # Budget Date is DD/MM/YYYY
        self.budget_df['Date'] = pd.to_datetime(self.budget_df['Date'], format='%d/%m/%Y')
        self.budget_month = self.budget_df[self.budget_df['Date'].dt.month == self.current_month].copy()
        
        logging.info(f"Budget month ({self.current_month}): {len(self.budget_month)} rows")
        logging.info(f"Budget month employees: {self.budget_month['Sales_Employee_Cleaned'].unique().tolist()}")
        
        # Filter Prior for Same Month Last Year
        # Prior Date is DD/MM/YYYY
        self.prior_df['Date'] = pd.to_datetime(self.prior_df['Date'], format='%d/%m/%Y')
        self.prior_month = self.prior_df[(self.prior_df['Date'].dt.year == self.prior_year) & (self.prior_df['Date'].dt.month == self.current_month)].copy()
        
    def _get_budget_value(self, salesperson):
        """Get budget value for a salesperson for the current month."""
        if salesperson in self.budget_month['Sales_Employee_Cleaned'].values:
            budget_row = self.budget_month[self.budget_month['Sales_Employee_Cleaned'] == salesperson]
            return budget_row['Value_kEUR'].iloc[0] if not budget_row.empty else 0
        return 0
        
    def _get_prior_value(self, salesperson):
        """Get prior year value for a salesperson for the same month."""
        if salesperson in self.prior_month['Sales_Employee_Cleaned'].values:
            prior_row = self.prior_month[self.prior_month['Sales_Employee_Cleaned'] == salesperson]
            return prior_row['Value_kEUR'].iloc[0] if not prior_row.empty else 0
        return 0
        
    def calculate_report(self):
        report_data = []
        section_totals = {}
        grand_total = {'Sales': 0, 'Budget': 0, 'Prior': 0}
        
        for section in self.config['sections']:
            if section.get('is_grand_total'):
                report_data.append({
                    'label': section['title'],
                    'sales': grand_total['Sales'],
                    'budget': grand_total['Budget'],
                    'prior': grand_total['Prior'],
                    'is_total': True,
                    'is_grand_total': True,
                    'is_spacer': False
                })
                continue
                
            if section.get('is_unmapped'):
                continue
                
            if section.get('is_total'):
                # Sum of other sections (e.g. Company 1 Total)
                t_sales = t_budget = t_prior = 0
                for comp in section['items'] if 'items' in section else section.get('components', []):
                    if comp in section_totals:
                        t_sales += section_totals[comp]['sales']
                        t_budget += section_totals[comp]['budget']
                        t_prior += section_totals[comp]['prior']
                
                report_data.append({
                    'label': section['title'],
                    'sales': t_sales,
                    'budget': t_budget,
                    'prior': t_prior,
                    'is_total': True,
                    'is_spacer': False,
                    'is_grand_total': False
                })
                
                # Add to grand total
                grand_total['Sales'] += t_sales
                grand_total['Budget'] += t_budget
                grand_total['Prior'] += t_prior
                continue

            # Regular Section
            sec_sales = 0
            sec_budget = 0
            sec_prior = 0
            
            rows = []
            fallback_indices = []
            known_filters = []
            section_region = section.get('title', '')
            
            if 'items' in section:
                # Section with items (sales employees)
                for item in section['items']:
                    label = item['label']
                    filter_val = item.get('filter_value')
                    is_fallback = item.get('is_fallback', False) or label.lower().startswith('other')
                    
                    if is_fallback:
                        rows.append({
                            'label': label,
                            'sales': 0.0,
                            'budget': 0.0,
                            'prior': 0.0,
                            'is_total': False,
                            'is_spacer': False
                        })
                        fallback_indices.append(len(rows) - 1)
                        continue

                    if filter_val:
                        known_filters.append(filter_val)
                        s_mask = (self.df['Sales_Employee_Cleaned'] == filter_val)
                        val_sales = self.df[s_mask]['kEUR'].sum()
                        val_budget = self._get_budget_value(filter_val)
                        val_prior = self._get_prior_value(filter_val)
                        
                        rows.append({
                            'label': label,
                            'sales': val_sales,
                            'budget': val_budget,
                            'prior': val_prior,
                            'is_total': False,
                            'is_spacer': False
                        })
                        
                        sec_sales += val_sales
                        sec_budget += val_budget
                        sec_prior += val_prior
            else:
                # Fallback for sections with sales_employee
                s_employee = section.get('sales_employee')
                if s_employee:
                    sales_mask = (self.df['Sales_Employee_Cleaned'] == s_employee)
                    sec_sales = self.df[sales_mask]['kEUR'].sum()
                    sec_budget = self._get_budget_value(s_employee)
                    sec_prior = 0
                    
                    rows.append({
                        'label': section['title'],
                        'sales': sec_sales,
                        'budget': sec_budget,
                        'prior': sec_prior,
                        'is_total': False,
                        'is_spacer': False
                    })
            
            # Fill fallback rows for "Other" categories using remaining Region totals
            # BEFORE adding to report_data
            if fallback_indices and section_region:
                region_mask = (self.df['Region'] == section_region)
                if known_filters:
                    region_mask &= (~self.df['Sales_Employee_Cleaned'].isin(known_filters) | self.df['Sales_Employee_Cleaned'].isna())

                fallback_sales = self.df[region_mask]['kEUR'].sum()

                budget_mask = (self.budget_month['Region'] == section_region) if 'Region' in self.budget_month.columns else None
                prior_mask = (self.prior_month['Region'] == section_region) if 'Region' in self.prior_month.columns else None

                if budget_mask is not None and known_filters:
                    budget_mask &= (~self.budget_month['Sales_Employee_Cleaned'].isin(known_filters) | self.budget_month['Sales_Employee_Cleaned'].isna())
                if prior_mask is not None and known_filters:
                    prior_mask &= (~self.prior_month['Sales_Employee_Cleaned'].isin(known_filters) | self.prior_month['Sales_Employee_Cleaned'].isna())

                fallback_budget = self.budget_month[budget_mask]['Value_kEUR'].sum() if budget_mask is not None else 0
                fallback_prior = self.prior_month[prior_mask]['Value_kEUR'].sum() if prior_mask is not None else 0

                for idx in fallback_indices:
                    rows[idx] = {
                        'label': rows[idx]['label'],
                        'sales': fallback_sales,
                        'budget': fallback_budget,
                        'prior': fallback_prior,
                        'is_total': False,
                        'is_spacer': False
                    }

                sec_sales += fallback_sales
                sec_budget += fallback_budget
                sec_prior += fallback_prior

            # Add rows to report (after fallback calculation)
            report_data.extend(rows)
            
            # Add Section Total if requested
            if section.get('show_total'):
                report_data.append({
                    'label': section['title'],
                    'sales': sec_sales,
                    'budget': sec_budget,
                    'prior': sec_prior,
                    'is_total': True,
                    'is_spacer': False
                })
                
            # Store for aggregation
            section_totals[section['title']] = {
                'sales': sec_sales,
                'budget': sec_budget,
                'prior': sec_prior
            }
            
            # Add spacer with consistent schema
            report_data.append({
                'label': '',
                'sales': 0.0,
                'budget': 0.0,
                'prior': 0.0,
                'is_spacer': True,
                'is_total': False,
                'is_grand_total': False
            })

        # Create DataFrame with explicit type enforcement
        df = pd.DataFrame(report_data)
        
        # Convert types directly (no fillna needed since all fields are explicitly set)
        df['is_spacer'] = df['is_spacer'].astype(bool)
        df['is_total'] = df['is_total'].astype(bool)
        df['is_grand_total'] = df['is_grand_total'].astype(bool)
        df['sales'] = df['sales'].astype(float)
        df['budget'] = df['budget'].astype(float)
        df['prior'] = df['prior'].astype(float)
        df['label'] = df['label'].astype(str)
        
        return df

    def render_report(self, df):
        # Print Header
        now = datetime.datetime.now()
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        col_curr = f"{month_name}-{year_short}A MTD"
        
        print(f"{'kEUR':<30} {col_curr:>15} {'Budget':>10} {'Prior':>10} {'% vs Bud':>10}")
        print("-" * 75)
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                print()
                continue
                
            label = row['label']
            sales = row['sales']
            budget = row['budget']
            prior = row['prior']
            
            # Add extra space above Company Sales totals
            if row.get('is_total') and 'Sales' in label:
                print()
            
            # Calculate %
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            # Format
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            print(f"{label:<30} {s_str:>10} {b_str:>10} {p_str:>10} {pct_str:>10}")
            
            if row.get('is_total') or row.get('is_grand_total'):
                print("-" * 75)
    
    # export_report() is inherited from BaseReportGenerator

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    # Load environment variables
    load_dotenv()

    # SharePoint configuration (use environment variables for security)
    SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
    CLIENT_ID = os.getenv('SHAREPOINT_CLIENT_ID')
    CLIENT_SECRET = os.getenv('SHAREPOINT_CLIENT_SECRET')
    
    project_root = Path(__file__).parent.parent
    
    use_sharepoint = all([SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET])
    
    if use_sharepoint:
        print("Starting GVL Report Generation with SharePoint Data")
        print("=" * 60)
        
        # Initialize progress
        total_steps = 5
        current_step = 0
        
        # Initialize SharePoint handler (suppress connection message)
        original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')
        try:
            sp_handler = SharePointHandler(SHAREPOINT_SITE_URL, CLIENT_ID, CLIENT_SECRET, quiet=True)
        finally:
            sys.stdout = original_stdout
        
        # Create temp directory for downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            current_step += 1
            print_progress(current_step, total_steps, "Downloading QRY files from SharePoint...")
            
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
                        pass  # Silent failure for individual files
            finally:
                sys.stdout = original_stdout  # Restore stdout
            
            print()  # Move to new line after progress bar
            print(f"[OK] Downloaded {downloaded_count} QRY files from SharePoint")
            
            current_step += 1
            print_progress(current_step, total_steps, "Processing QRY data...")
            qry_df = process_qry_files(temp_dir)
            
            current_step += 1
            print_progress(current_step, total_steps, "Downloading support files...")
            
            # Define other SharePoint paths
            current_year = get_current_year()
            prior_year = get_prior_year()
            other_paths = {
                'mapping': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/entity_mappings.csv',
                'budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_GVL_{current_year}.csv',
                'prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_{prior_year}_gvl.csv'
            }
            
            local_paths = {}
            original_stdout = sys.stdout
            sys.stdout = open(os.devnull, 'w')  # Suppress prints during downloads
            try:
                for key, sp_path in other_paths.items():
                    local_path = os.path.join(temp_dir, os.path.basename(sp_path))
                    try:
                        sp_handler.download_file(sp_path, local_path)
                        local_paths[key] = local_path
                    except Exception as e:
                        # Fallback to local paths
                        if key == 'mapping':
                            local_paths[key] = str(project_root / 'data/inputs/mappings/entity_mappings.csv')
                        if key == 'budget':
                            local_paths[key] = str(project_root / f'data/inputs/budget/budget_GVL_{current_year}.csv')
                        elif key == 'prior':
                            local_paths[key] = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv')
            finally:
                sys.stdout = original_stdout  # Restore stdout
            
            current_step += 1
            print_progress(current_step, total_steps, "Applying entity mappings...")
            mapping_df = pd.read_csv(local_paths['mapping'])
            mapped_df = apply_mappings(qry_df, mapping_df)
            
            current_step += 1
            print_progress(current_step, total_steps, "Generating management report...")
            
            # Save mapped data locally for reference/debugging
            current_year = get_current_year()
            mapped_path = os.path.join(temp_dir, f'qry_unified_mapped_{current_year}.csv')
            mapped_df.to_csv(mapped_path, index=False)
            
            # Run the report generator with processed data
            generator = GVLReportGenerator(
                str(project_root / 'src/config/gvl_report_structure.json'),
                mapped_path,
                local_paths['budget'],
                local_paths['prior']
            )
            df = generator.calculate_report()
            print()  # Move to new line before report output
            generator.render_report(df)
            
            # Generate timestamped filename
            now = datetime.datetime.now()
            timestamp = now.strftime('%Y%m%d_%H%M%S')
            current_year = get_current_year()
            base_filename = f'management_report_gvl_{current_year}_{timestamp}'
            
            # Export to data/outputs folder
            output_dir = str(project_root / 'data/outputs')
            os.makedirs(output_dir, exist_ok=True)
            local_base_path = os.path.join(output_dir, base_filename)
            generator.export_report(df, local_base_path + '.csv')
            
            print(f"\n[SUCCESS] Report generation complete! Files saved to {output_dir}/")
            print(f"[INFO] Exported in 4 formats: CSV, TXT, HTML, and PDF")
            print(f"[INFO] Processed {len(qry_df)} QRY records -> {len(mapped_df)} mapped records")
            print("=" * 60)
    else:
        print("SharePoint credentials not found. Using local files.")
        # Fallback to local file processing
        project_root = Path(__file__).parent.parent
        current_year = get_current_year()
        prior_year = get_prior_year()
        generator = GVLReportGenerator(
            project_root / 'src/config/gvl_report_structure.json',
            project_root / f'data/outputs/qry_unified_mapped_{current_year}.csv',
            project_root / f'data/inputs/budget/budget_2025_processed.csv',
            project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv'
        )
        df = generator.calculate_report()
        generator.render_report(df)
        
        # Generate timestamped filename
        now = datetime.datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        current_year = get_current_year()
        output_path = project_root / f'data/outputs/management_report_gvl_{current_year}_{timestamp}.csv'
        
        generator.export_report(df, output_path)
    
    end_time = datetime.datetime.now()
    print("\nGenerator runtime: {:.2f} seconds".format((end_time - start_time).total_seconds()))