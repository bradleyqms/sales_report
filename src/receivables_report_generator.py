import pandas as pd
import json
import datetime
import os
import tempfile
import sys
import time
import logging
import warnings
from pathlib import Path
from dotenv import load_dotenv
from typing import List

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

class ManagementReportGenerator(BaseReportGenerator):
    """
    Management Report Generator for QRY sales data.
    
    Generates reports comparing current sales vs budget vs prior year
    broken down by Company Group, Market Group, Region, and Channel.
    """
    
    def __init__(self, config_path, sales_path, budget_path, prior_path):
        # Call parent constructor (loads config, data files, prepares dates)
        super().__init__(config_path, sales_path, budget_path, prior_path)
        self._prepare_data()
        
    def _prepare_data(self):
        """Prepare sales, budget, and prior data for report generation."""
        # Filter Sales to AR and CN (Credit Notes)
        # CN values already have minus signs, so they will be subtracted from totals
        self.df = self.df[self.df['Document Type'].isin(['AR', 'CN'])].copy()
        
        # Exclude Interco transactions from Management Report
        # Check both Market_Group and Sales_Employee_Cleaned columns
        self.df = self.df[
            (self.df['Market_Group'] != 'Interco') & 
            (self.df['Sales_Employee_Cleaned'] != 'Interco')
        ].copy()
        
        # Also exclude rows where Channel_Level is 'Interco'
        if 'Channel_Level' in self.df.columns:
            self.df = self.df[self.df['Channel_Level'] != 'Interco'].copy()
        
        # Convert Sales to kEUR
        self._convert_to_keur()
        
        # Check if USA-specific budget/prior files are available, and prefer them for USA sections
        repo_root = Path(__file__).parent.parent
        usa_budget_path = repo_root / f'data/inputs/budget/budget_USA_spa_{self.current_year}.csv'
        usa_prior_path = repo_root / f'data/inputs/prior_years/prior_sales_{self.prior_year}_usa.csv'
        
        # Load USA-specific budget if available, otherwise use provided budget
        if usa_budget_path.exists():
            try:
                self.usa_budget_df = pd.read_csv(usa_budget_path)
                logging.info(f"[DEBUG] Loaded USA-specific budget file: {usa_budget_path.name}")
            except Exception as e:
                logging.warning(f"[DEBUG] Failed to load USA budget file: {e}")
                self.usa_budget_df = None
        else:
            self.usa_budget_df = None
        
        # Load USA-specific prior if available, otherwise use provided prior
        if usa_prior_path.exists():
            try:
                self.usa_prior_df = pd.read_csv(usa_prior_path)
                logging.info(f"[DEBUG] Loaded USA-specific prior file: {usa_prior_path.name}")
            except Exception as e:
                logging.warning(f"[DEBUG] Failed to load USA prior file: {e}")
                self.usa_prior_df = None
        else:
            self.usa_prior_df = None
        
        # Filter Budget and Prior for Current Month using base class methods
        self.budget_month = self._filter_budget_for_month()
        self.prior_month = self._filter_prior_for_month()
        
        logging.info(f"[DEBUG] Prior month filtered records: {len(self.prior_month)}")
        logging.info(f"[DEBUG] Prior month sample:\n{self.prior_month.head()}")

        # Quick sanity check: totals by Region for current month/prior year
        if 'Region' in self.prior_month.columns and 'Value_kEUR' in self.prior_month.columns:
            region_totals = self.prior_month.groupby('Region')['Value_kEUR'].sum().sort_values(ascending=False)
            logging.info(f"[DEBUG] Prior month totals by Region:\n{region_totals}")
        
        # Debugging information
        logging.info(f"[DEBUG] Prior year file columns: {list(self.prior_df.columns)}")
        logging.info(f"[DEBUG] Prior year file shape: {self.prior_df.shape}")
        logging.info(f"[DEBUG] Prior year unique years: {self.prior_df['Year'].unique() if 'Year' in self.prior_df.columns else 'No Year column'}")
        logging.info(f"[DEBUG] Current month for filtering: {get_current_month()}")
        
        # After filtering by month
        logging.info(f"[DEBUG] Prior year data after month filter: {len(self.prior_month)} records")
        logging.info(f"[DEBUG] Prior year sample (filtered):\n{self.prior_month.head()}")
    
    def get_report_headers(self) -> List[str]:
        """Return column headers for the management report."""
        # Use the anchored report date if available, otherwise current date
        now = getattr(self, 'now', datetime.datetime.now())
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        
        # Detect if this is EOM (last day of month) or MTD
        import calendar
        last_day = calendar.monthrange(now.year, now.month)[1]
        period_type = 'EOM' if now.day == last_day else 'MTD'
        
        return ['kEUR', f'{month_name}-{year_short}A {period_type}', 'Budget', 'Prior', '% vs Bud']
    
    def get_report_title(self) -> str:
        """Return the report title."""
        return "QRY Management Report"
    
    def format_row_for_export(self, row: pd.Series) -> List[str]:
        """Format a row for export to CSV/TXT/HTML/PDF."""
        label = row['label']
        sales = row['sales']
        budget = row['budget']
        prior = row['prior']
        
        pct = (sales / budget * 100) if budget and budget != 0 else 0
        
        def _fmt(v):
            if abs(v) >= 1:    return f"{int(round(v))}"
            if 0 < abs(v) < 1: return f"{v:.1f}"
            if v == 0:         return "-"
            return "0"

        s_str = _fmt(sales)
        b_str = _fmt(budget)
        p_str = _fmt(prior)
        pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
        
        return [label, s_str, b_str, p_str, pct_str]
        
    def calculate_report(self):
        report_data = []
        section_totals = {}
        grand_total = {'Sales': 0, 'Budget': 0, 'Prior': 0}
        
        for section in self.config['sections']:
            if section.get('is_grand_total'):
                # Before outputting grand total, subtract any company-level totals
                # so that grand total reflects base items only.
                deduction_sales = 0
                deduction_budget = 0
                deduction_prior = 0
                for key,vals in section_totals.items():
                    # Match titles like 'Company 1 Sales', 'Company 2 Sales', etc.
                    if isinstance(key, str) and key.startswith('Company ') and key.endswith(' Sales'):
                        deduction_sales += vals.get('sales', 0)
                        deduction_budget += vals.get('budget', 0)
                        deduction_prior += vals.get('prior', 0)

                adj_sales = grand_total['Sales'] - deduction_sales
                adj_budget = grand_total['Budget'] - deduction_budget
                adj_prior = grand_total['Prior'] - deduction_prior

                report_data.append({
                    'label': section['title'],
                    'sales': adj_sales,
                    'budget': adj_budget,
                    'prior': adj_prior,
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
            
            # Get Section Totals first (for fallback calculation)
            # Filter by Company Group and Market Group
            c_group = section.get('company_group')
            m_group = section.get('market_group')
            
            # Base filters for the whole section
            sales_mask = (self.df['Company_Group'] == c_group)
            budget_mask = (self.budget_month['Company_Group'] == c_group)
            prior_mask = (self.prior_month['Company_Group'] == c_group)
            
            if m_group:
                sales_mask &= (self.df['Market_Group'] == m_group)
                budget_mask &= (self.budget_month['Market_Group'] == m_group)
                prior_mask &= (self.prior_month['Market_Group'] == m_group)
            
            section_total_sales = self.df[sales_mask]['kEUR'].sum()

            # For USA sections, prefer USA-specific budget/prior only for region-based sections
            if m_group == 'USA' and self.usa_budget_df is not None and section.get('type') == 'region':
                # Regions defined in the section items (type == 'region')
                usa_regions = [it.get('filter_value') for it in section.get('items', []) if it.get('filter_value')]
                usa_budget_month = self.usa_budget_df.copy()
                usa_budget_month['Date'] = pd.to_datetime(usa_budget_month['Date'], format='%d/%m/%Y', errors='coerce')
                usa_budget_month = usa_budget_month[usa_budget_month['Date'].dt.month == self.current_month]
                if usa_regions:
                    usa_budget_month = usa_budget_month[usa_budget_month['Region'].isin(usa_regions)]
                # Parse numerics
                if 'Value_kUSD' in usa_budget_month.columns:
                    usa_budget_month['Value_kUSD'] = pd.to_numeric(usa_budget_month['Value_kUSD'], errors='coerce').fillna(0)
                if 'Value_kEUR' in usa_budget_month.columns:
                    usa_budget_month['Value_kEUR'] = pd.to_numeric(usa_budget_month['Value_kEUR'], errors='coerce').fillna(0)
                section_total_budget = usa_budget_month['Value_kUSD'].sum() if 'Value_kUSD' in usa_budget_month.columns else usa_budget_month['Value_kEUR'].sum()
            elif m_group == 'USA' and 'Value_kUSD' in self.budget_month.columns:
                section_total_budget = self.budget_month[budget_mask]['Value_kUSD'].sum()
            else:
                section_total_budget = self.budget_month[budget_mask]['Value_kEUR'].sum()

            if m_group == 'USA' and self.usa_prior_df is not None and section.get('type') == 'region':
                usa_prior_month = self.usa_prior_df.copy()
                usa_prior_month['Date'] = pd.to_datetime(usa_prior_month['Date'], format='%d/%m/%Y', errors='coerce')
                usa_prior_month = usa_prior_month[(usa_prior_month['Date'].dt.year == self.prior_year) & (usa_prior_month['Date'].dt.month == self.current_month)]
                usa_regions = [it.get('filter_value') for it in section.get('items', []) if it.get('filter_value')]
                if usa_regions:
                    usa_prior_month = usa_prior_month[usa_prior_month['Region'].isin(usa_regions)]
                if 'Value_kUSD' in usa_prior_month.columns:
                    usa_prior_month['Value_kUSD'] = pd.to_numeric(usa_prior_month['Value_kUSD'], errors='coerce').fillna(0)
                if 'Value_kEUR' in usa_prior_month.columns:
                    usa_prior_month['Value_kEUR'] = pd.to_numeric(usa_prior_month['Value_kEUR'], errors='coerce').fillna(0)
                section_total_prior = usa_prior_month['Value_kUSD'].sum() if 'Value_kUSD' in usa_prior_month.columns else usa_prior_month['Value_kEUR'].sum()
            else:
                section_total_prior = self.prior_month[prior_mask]['Value_kEUR'].sum()
            
            # Track allocated amounts to calculate fallback
            allocated_sales = 0
            allocated_budget = 0
            allocated_prior = 0
            
            rows = []
            
            for item in section.get('items', []):
                label = item['label']
                is_fallback = item.get('is_fallback', False)
                
                if is_fallback:
                    # Will calculate at end of loop
                    rows.append({'label': label, 'type': 'fallback'})
                    continue
                
                # Item specific filters
                filter_val = item.get('filter_value')
                filter_type = section.get('type') # 'region' or 'channel'
                
                # Sales Filter
                s_mask = sales_mask.copy()
                if filter_type == 'region':
                    s_mask &= (self.df['Region'] == filter_val)
                elif filter_type == 'channel':
                    s_mask &= (self.df['Channel_Level'] == filter_val)
                
                val_sales = self.df[s_mask]['kEUR'].sum()
                
                # Budget/Prior Filter
                # Check for override map (e.g. Company 3 channels mapping to regions)
                b_filter_val = item.get('budget_region_map', filter_val)
                
                b_mask = budget_mask.copy()
                p_mask = prior_mask.copy()
                
                lookup_col = 'Region' if 'budget_region_map' in item else ('Region' if filter_type == 'region' else 'Channel_Level')
                
                b_mask &= (self.budget_month[lookup_col] == b_filter_val)
                p_mask &= (self.prior_month[lookup_col] == b_filter_val)

                # For USA region items, compute from USA-specific datasets if available
                if m_group == 'USA' and section.get('type') == 'region' and self.usa_budget_df is not None:
                    usa_budget_month = self.usa_budget_df.copy()
                    usa_budget_month['Date'] = pd.to_datetime(usa_budget_month['Date'], format='%d/%m/%Y', errors='coerce')
                    usa_budget_month = usa_budget_month[usa_budget_month['Date'].dt.month == self.current_month]
                    usa_budget_month = usa_budget_month[usa_budget_month['Region'] == filter_val]
                    if 'Value_kUSD' in usa_budget_month.columns:
                        usa_budget_month['Value_kUSD'] = pd.to_numeric(usa_budget_month['Value_kUSD'], errors='coerce').fillna(0)
                        val_budget = usa_budget_month['Value_kUSD'].sum()
                    else:
                        usa_budget_month['Value_kEUR'] = pd.to_numeric(usa_budget_month['Value_kEUR'], errors='coerce').fillna(0)
                        val_budget = usa_budget_month['Value_kEUR'].sum()
                elif m_group == 'USA' and 'Value_kUSD' in self.budget_month.columns:
                    val_budget = self.budget_month[b_mask]['Value_kUSD'].sum()
                else:
                    val_budget = self.budget_month[b_mask]['Value_kEUR'].sum()

                if m_group == 'USA' and section.get('type') == 'region' and self.usa_prior_df is not None:
                    usa_prior_month = self.usa_prior_df.copy()
                    usa_prior_month['Date'] = pd.to_datetime(usa_prior_month['Date'], format='%d/%m/%Y', errors='coerce')
                    usa_prior_month = usa_prior_month[(usa_prior_month['Date'].dt.year == self.prior_year) & (usa_prior_month['Date'].dt.month == self.current_month)]
                    usa_prior_month = usa_prior_month[usa_prior_month['Region'] == filter_val]
                    if 'Value_kUSD' in usa_prior_month.columns:
                        usa_prior_month['Value_kUSD'] = pd.to_numeric(usa_prior_month['Value_kUSD'], errors='coerce').fillna(0)
                        val_prior = usa_prior_month['Value_kUSD'].sum()
                    else:
                        usa_prior_month['Value_kEUR'] = pd.to_numeric(usa_prior_month['Value_kEUR'], errors='coerce').fillna(0)
                        val_prior = usa_prior_month['Value_kEUR'].sum()
                else:
                    val_prior = self.prior_month[p_mask]['Value_kEUR'].sum()
                
                # Check if item has explicit bold config (default True if not specified)
                should_bold = item.get('bold', True)
                
                rows.append({
                    'label': label,
                    'sales': val_sales,
                    'budget': val_budget,
                    'prior': val_prior,
                    'is_total': False,
                    'should_bold': should_bold,  # Explicit styling hint
                    'is_spacer': False
                })
                
                allocated_sales += val_sales
                allocated_budget += val_budget
                allocated_prior += val_prior
            
            # Process Fallback
            for i, row in enumerate(rows):
                if row.get('type') == 'fallback':
                    rem_sales = section_total_sales - allocated_sales
                    rem_budget = section_total_budget - allocated_budget
                    rem_prior = section_total_prior - allocated_prior
                    
                    # Only show if there is value
                    if abs(rem_sales) > 0.1 or abs(rem_budget) > 0.1 or abs(rem_prior) > 0.1:
                        rows[i] = {
                            'label': row['label'],
                            'sales': rem_sales,
                            'budget': rem_budget,
                            'prior': rem_prior,
                            'is_total': False,
                            'should_bold': True,
                            'is_spacer': False
                        }
                    else:
                        rows[i] = None # Mark for removal
            
            # Add rows to report
            rows = [r for r in rows if r is not None]
            report_data.extend(rows)
            
            # Add Section Total if requested or if it's a component
            if section.get('show_total') or section.get('title') in ['Core Markets', 'UK', 'USA', 'Export']:
                report_data.append({
                    'label': f"Total {section['title']}",
                    'sales': section_total_sales,
                    'budget': section_total_budget,
                    'prior': section_total_prior,
                    'is_total': True,
                    'is_spacer': False,
                    'is_grand_total': False
                })
                
            # Store for aggregation
            section_totals[section['title']] = {
                'sales': section_total_sales,
                'budget': section_total_budget,
                'prior': section_total_prior
            }
            
            # Add spacer with consistent schema
            report_data.append({
                'label': '',
                'sales': 0.0,
                'budget': 0.0,
                'prior': 0.0,
                'is_spacer': True,
                'is_total': False,
                'is_grand_total': False,
                'should_bold': True
            })
            
            # Add to grand total for company sales sections
            if 'Sales' in section['title']:
                grand_total['Sales'] += section_total_sales
                grand_total['Budget'] += section_total_budget
                grand_total['Prior'] += section_total_prior

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
        now = getattr(self, 'now', datetime.datetime.now())
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        
        # Detect if this is EOM (last day of month) or MTD
        import calendar
        last_day = calendar.monthrange(now.year, now.month)[1]
        period_type = 'EOM' if now.day == last_day else 'MTD'
        col_curr = f"{month_name}-{year_short}A {period_type}"
        
        print(f"\n{'kEUR':<30} {col_curr:>15} {'Budget':>10} {'Prior':>10} {'% vs Bud':>10}")
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
            
            # Format values (already in kEUR)
            def _fmt(v):
                if abs(v) >= 1:    return f"{int(round(v))}"
                if 0 < abs(v) < 1: return f"{v:.1f}"
                if v == 0:         return "-"
                return "0"

            s_str = _fmt(sales)
            b_str = _fmt(budget)
            p_str = _fmt(prior)
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
        print("Starting QRY Report Generation with SharePoint Data")
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
                'budget': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_{current_year}_processed.csv',
                'prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_{prior_year}_processed.csv'
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
                            local_paths[key] = str(project_root / f'data/inputs/budget/budget_{current_year}_processed.csv')
                        elif key == 'prior':
                            local_paths[key] = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv')
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
            generator = ManagementReportGenerator(
                str(project_root / 'src/config/report_structure.json'),
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
            base_filename = f'management_report_qry_{current_year}_{timestamp}'
            
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
        generator = ManagementReportGenerator(
            project_root / 'src/config/report_structure.json',
            project_root / f'data/outputs/qry_unified_mapped_{current_year}.csv',
            project_root / f'data/inputs/budget/budget_{current_year}_processed.csv',
            project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv'
        )
        df = generator.calculate_report()
        generator.render_report(df)
        
        # Generate timestamped filename
        now = datetime.datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        current_year = get_current_year()
        output_path = project_root / f'data/outputs/management_report_qry_{current_year}_{timestamp}.csv'
        
        generator.export_report(df, output_path)
    
    end_time = datetime.datetime.now()
    print("\nGenerator runtime: {:.2f} seconds".format((end_time - start_time).total_seconds()))