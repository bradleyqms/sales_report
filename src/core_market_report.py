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

class CoreMarketReportGenerator(BaseReportGenerator):
    """
    Core Market Report Generator for European sub-regions.
    
    Generates reports for Core Markets (Germany, Benelux, Switzerland, etc.)
    with Existing vs New customer breakdowns.
    """
    
    def __init__(self, config_path, sales_path, budget_path, prior_path, split_summary_path=None,
                 report_date=None, py_mapping_path=None, entity_mapping_path=None):
        # Call parent constructor (loads config, data files, prepares dates)
        super().__init__(config_path, sales_path, budget_path, prior_path,
                         report_date=report_date)
        
        # Store mapping paths for use in _prepare_data (fall back to default local paths if not provided)
        self._py_mapping_path = py_mapping_path
        self._entity_mapping_path = entity_mapping_path
        
        # Load sales split summary if available (CoreMarket-specific)
        self.split_summary = None
        if split_summary_path and os.path.exists(split_summary_path):
            try:
                self.split_summary = pd.read_csv(split_summary_path)
                logging.info(f"Loaded sales split summary from {split_summary_path}")
            except Exception as e:
                logging.warning(f"Could not load sales split summary: {e}")
        
        self._prepare_data()
    
    def get_report_headers(self) -> List[str]:
        """Return column headers for the Core Market report."""
        return ['kEUR', 'Total Sales', 'Existing', 'New', 'Total Budget', 'Existing Budget', 'New Budget', 'Prior YoY', '% vs Budget']
    
    def get_report_title(self) -> str:
        """Return the report title."""
        return "Core Markets Report"
    
    def format_row_for_export(self, row: pd.Series) -> List[str]:
        """Format a row for export to CSV/TXT/HTML/PDF."""
        label = row['label']
        sales = row['sales']
        existing_sales = row['existing_sales']
        new_sales = row['new_sales']
        budget = row['budget']
        existing_budget = row['existing_budget']
        new_budget = row['new_budget']
        prior = row['prior']
        
        pct = (sales / budget * 100) if budget and budget != 0 else 0
        
        def _fmt(v):
            if abs(v) >= 1:    return f"{int(round(v))}"
            if 0 < abs(v) < 1: return f"{v:.1f}"
            if v == 0:         return "-"
            return "0"

        s_str   = _fmt(sales)
        ex_str  = _fmt(existing_sales)
        new_str = _fmt(new_sales)
        b_str   = _fmt(budget)
        exb_str = _fmt(existing_budget)
        newb_str = _fmt(new_budget)
        p_str   = _fmt(prior)
        pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
        
        return [label, s_str, ex_str, new_str, b_str, exb_str, newb_str, p_str, pct_str]
            
    def _prepare_data(self):
        # Filter Sales to AR and CN (Credit Notes)
        # CN values already have minus signs, so they will be subtracted from totals
        self.df = self.df[self.df['Document Type'].isin(['AR', 'CN'])].copy()
        
        # Convert Sales to kEUR
        value_col = 'Value_in_EUR_converted' if 'Value_in_EUR_converted' in self.df.columns else 'Total Value (EUR)'
        self.df['kEUR'] = self.df[value_col].fillna(0) / 1000
        
        # Tag sales rows as existing or new based on "Neukd" designation in Sales Employee Name
        if 'Sales Employee Name' in self.df.columns:
            self.df['is_neukd'] = self.df['Sales Employee Name'].fillna('').str.lower().str.contains('neukd')
        else:
            self.df['is_neukd'] = False
        
        # Clean Sub_Region in budget and prior - handle both 'Sub Region' and 'Subchannel / Partner' columns
        # Clean Sub_Region in budget and prior - coalesce 'Sub Region' with 'Subchannel / Partner'
        # so that whichever column is populated in the source file is used.
        def _coalesce_sub_region(df):
            sr = df['Sub Region'].fillna('').str.strip() if 'Sub Region' in df.columns else pd.Series('', index=df.index)
            sc = df['Subchannel / Partner'].fillna('').str.strip() if 'Subchannel / Partner' in df.columns else pd.Series('', index=df.index)
            return sr.where(sr != '', sc)

        self.budget_df['Sub_Region_Cleaned'] = _coalesce_sub_region(self.budget_df)
        self.prior_df['Sub_Region_Cleaned'] = _coalesce_sub_region(self.prior_df)
        
        # Derive Sales_Employee_Cleaned directly from Sales Employee / Account for prior data.
        # GVL-format prior files use rep names (e.g. "Kerstin") as the account value;
        # entity_mappings covers QRY sales data and won't have these names, so we use the
        # raw value rather than going through entity_mappings (which would yield empty strings).
        if 'Sales Employee / Account' in self.prior_df.columns:
            self.prior_df['Sales_Employee_Cleaned'] = self.prior_df['Sales Employee / Account'].fillna('').str.strip()
        else:
            self.prior_df['Sales_Employee_Cleaned'] = ''

        # Apply PY-specific regional mappings: override Sub_Region_Cleaned wherever a match
        # exists in py25_regional_mappings (e.g. "Kerstin" → "North", "Marina" → "NRW").
        # This handles GVL-format prior data where Subchannel / Partner holds employee names.
        py_mapping_path = Path(self._py_mapping_path) if self._py_mapping_path else Path(__file__).parent.parent / 'data/inputs/mappings/py25_regional_mappings.csv'
        if Path(py_mapping_path).exists():
            py_mappings_df = pd.read_csv(py_mapping_path)
            py_mappings = py_mappings_df.set_index('Sales_Employee_Cleaned')['Sub_Region'].to_dict()
            mapped_region = self.prior_df['Sales_Employee_Cleaned'].map(py_mappings)
            has_mapping = mapped_region.notna()
            self.prior_df.loc[has_mapping, 'Sub_Region_Cleaned'] = mapped_region[has_mapping]

        # Fallback to entity mappings for any Sub_Region_Cleaned that is still empty
        entity_mapping_path = Path(self._entity_mapping_path) if self._entity_mapping_path else Path(__file__).parent.parent / 'data/inputs/mappings/entity_mappings.csv'
        if Path(entity_mapping_path).exists():
            entity_mappings_df = pd.read_csv(entity_mapping_path)
            entity_mappings = entity_mappings_df.set_index('Sales_Employee_Cleaned')['Sub Region'].dropna().to_dict()
            mask = self.prior_df['Sub_Region_Cleaned'] == ''
            self.prior_df.loc[mask, 'Sub_Region_Cleaned'] = self.prior_df.loc[mask, 'Sales_Employee_Cleaned'].map(entity_mappings).fillna('')
        
        # Filter Budget for Current Month
        # Budget Date is DD/MM/YYYY
        self.budget_df['Date'] = pd.to_datetime(self.budget_df['Date'], format='%d/%m/%Y')
        self.budget_month = self.budget_df[self.budget_df['Date'].dt.month == self.current_month].copy()
        
        # Ensure budget numeric columns are parsed correctly
        if 'Value_kEUR' in self.budget_month.columns:
            self.budget_month['Value_kEUR'] = pd.to_numeric(self.budget_month['Value_kEUR'], errors='coerce').fillna(0)

        def _coerce_numeric(series):
            return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        if 'Existing_Budget_kEUR' in self.budget_month.columns:
            self.budget_month['Existing_Budget_kEUR'] = _coerce_numeric(self.budget_month['Existing_Budget_kEUR'])
        elif 'Existing_Budget_EUR' in self.budget_month.columns:
            self.budget_month['Existing_Budget_kEUR'] = _coerce_numeric(self.budget_month['Existing_Budget_EUR']) / 1000

        if 'New_Budget_kEUR' in self.budget_month.columns:
            self.budget_month['New_Budget_kEUR'] = _coerce_numeric(self.budget_month['New_Budget_kEUR'])
        elif 'New_Budget_EUR' in self.budget_month.columns:
            self.budget_month['New_Budget_kEUR'] = _coerce_numeric(self.budget_month['New_Budget_EUR']) / 1000
        
        # Filter Prior for Same Month Last Year
        # Prior Date is DD/MM/YYYY
        self.prior_df['Date'] = pd.to_datetime(self.prior_df['Date'], format='%d/%m/%Y')
        self.prior_month = self.prior_df[(self.prior_df['Date'].dt.year == self.prior_year) & (self.prior_df['Date'].dt.month == self.current_month)].copy()
        
        # Ensure prior numeric columns are parsed correctly
        if 'Value_kEUR' in self.prior_month.columns:
            self.prior_month['Value_kEUR'] = pd.to_numeric(self.prior_month['Value_kEUR'], errors='coerce').fillna(0)
        
        # Create Sub Region to Company Entity mapping based on config
        self.sub_region_to_entity = {}
        for section in self.config.get('sections', []):
            # Map section title to Company Entity
            section_title = section.get('title', '')
            if section_title == 'Germany':
                entity = 'GmbH'
            elif section_title == 'Switzerland':
                entity = 'AG'
            else:
                entity = None  # Other regions handled separately
            
            # Map each sub-region item
            if 'items' in section and entity:
                for item in section['items']:
                    filter_val = item.get('filter_value')
                    if filter_val:
                        self.sub_region_to_entity[filter_val] = entity
        
        # Pre-build lookup dictionaries for fast access
        self.budget_lookup = {}
        if 'Sub_Region_Cleaned' in self.budget_month.columns and 'Value_kEUR' in self.budget_month.columns:
            for _, row in self.budget_month.iterrows():
                sub_region = row['Sub_Region_Cleaned']
                if sub_region and pd.notna(sub_region) and sub_region.strip():
                    if sub_region not in self.budget_lookup:
                        self.budget_lookup[sub_region] = 0
                    self.budget_lookup[sub_region] += row['Value_kEUR']

        self.existing_budget_lookup = {}
        if 'Sub_Region_Cleaned' in self.budget_month.columns and 'Existing_Budget_kEUR' in self.budget_month.columns:
            for _, row in self.budget_month.iterrows():
                sub_region = row['Sub_Region_Cleaned']
                if sub_region and pd.notna(sub_region) and sub_region.strip():
                    if sub_region not in self.existing_budget_lookup:
                        self.existing_budget_lookup[sub_region] = 0
                    self.existing_budget_lookup[sub_region] += row['Existing_Budget_kEUR']

        self.new_budget_lookup = {}
        if 'Sub_Region_Cleaned' in self.budget_month.columns and 'New_Budget_kEUR' in self.budget_month.columns:
            for _, row in self.budget_month.iterrows():
                sub_region = row['Sub_Region_Cleaned']
                if sub_region and pd.notna(sub_region) and sub_region.strip():
                    if sub_region not in self.new_budget_lookup:
                        self.new_budget_lookup[sub_region] = 0
                    self.new_budget_lookup[sub_region] += row['New_Budget_kEUR']
        
        self.prior_lookup = {}
        if 'Sub_Region_Cleaned' in self.prior_month.columns and 'Value_kEUR' in self.prior_month.columns:
            for _, row in self.prior_month.iterrows():
                sub_region = row['Sub_Region_Cleaned']
                if sub_region and pd.notna(sub_region) and sub_region.strip():
                    if sub_region not in self.prior_lookup:
                        self.prior_lookup[sub_region] = 0
                    self.prior_lookup[sub_region] += row['Value_kEUR']
        
    def _get_budget_value(self, sub_region):
        """Get budget value for a sub-region for the current month."""
        return self.budget_lookup.get(sub_region, 0)

    def _get_existing_budget_value(self, sub_region):
        """Get existing-customer budget value for a sub-region for the current month."""
        return self.existing_budget_lookup.get(sub_region, 0)

    def _get_new_budget_value(self, sub_region):
        """Get new-customer budget value for a sub-region for the current month."""
        return self.new_budget_lookup.get(sub_region, 0)
        
    def _get_prior_value(self, sub_region):
        """Get prior year value for a sub-region for the same month."""
        return self.prior_lookup.get(sub_region, 0)
        
    def _get_sales_by_type(self, entity, doc_type, sales_type):
        """
        Get sales value by employee type (neukd or regular) from split summary.
        entity: 'AG' or 'GmbH'
        doc_type: e.g., 'AR', 'CN', 'SO_OPEN', 'SO_TOTAL'
        sales_type: 'neukd' or 'regular'
        """
        if self.split_summary is None or self.split_summary.empty:
            return 0.0
        
        # Filter by entity and document type
        mask = (self.split_summary['Company Entity'] == entity) & (self.split_summary['Document Type'] == doc_type)
        filtered = self.split_summary[mask]
        
        if filtered.empty:
            return 0.0
        
        # Sum the appropriate column
        if sales_type == 'neukd':
            return filtered['neukd_sales'].sum()
        elif sales_type == 'regular':
            return filtered['regular_sales'].sum()
        else:
            return 0.0
        
    def calculate_report(self):
        report_data = []
        section_totals = {}
        grand_total = {
            'Sales': 0,
            'Existing_Sales': 0,
            'New_Sales': 0,
            'Budget': 0,
            'Existing_Budget': 0,
            'New_Budget': 0,
            'Prior': 0
        }
        
        for section in self.config['sections']:
            if section.get('is_grand_total'):
                report_data.append({
                    'label': section['title'],
                    'sales': grand_total['Sales'],
                    'existing_sales': grand_total['Existing_Sales'],
                    'new_sales': grand_total['New_Sales'],
                    'budget': grand_total['Budget'],
                    'existing_budget': grand_total['Existing_Budget'],
                    'new_budget': grand_total['New_Budget'],
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
                t_sales = t_existing_sales = t_new_sales = t_budget = t_existing_budget = t_new_budget = t_prior = 0
                for comp in section['items'] if 'items' in section else section.get('components', []):
                    if comp in section_totals:
                        t_sales += section_totals[comp]['sales']
                        t_existing_sales += section_totals[comp].get('existing_sales', 0)
                        t_new_sales += section_totals[comp].get('new_sales', 0)
                        t_budget += section_totals[comp]['budget']
                        t_existing_budget += section_totals[comp].get('existing_budget', 0)
                        t_new_budget += section_totals[comp].get('new_budget', 0)
                        t_prior += section_totals[comp]['prior']
                
                report_data.append({
                    'label': section['title'],
                    'sales': t_sales,
                    'existing_sales': t_existing_sales,
                    'new_sales': t_new_sales,
                    'budget': t_budget,
                    'existing_budget': t_existing_budget,
                    'new_budget': t_new_budget,
                    'prior': t_prior,
                    'is_total': True,
                    'is_spacer': False,
                    'is_grand_total': False
                })
                
                # Add to grand total
                grand_total['Sales'] += t_sales
                grand_total['Existing_Sales'] += t_existing_sales
                grand_total['New_Sales'] += t_new_sales
                grand_total['Budget'] += t_budget
                grand_total['Existing_Budget'] += t_existing_budget
                grand_total['New_Budget'] += t_new_budget
                grand_total['Prior'] += t_prior
                continue

            # Regular Section
            sec_sales = 0
            sec_budget = 0
            sec_existing_budget = 0
            sec_new_budget = 0
            sec_prior = 0
            
            rows = []
            
            if 'items' in section:
                # Section with items (sub-regions)
                for item in section['items']:
                    label = item['label']
                    filter_val = item.get('filter_value')
                    
                    if filter_val:
                        s_mask = (self.df['Sub Region'] == filter_val)
                        val_sales = self.df[s_mask]['kEUR'].sum()
                        
                        # Aggregate existing vs new sales separately based on is_neukd flag
                        val_existing_sales = self.df[s_mask & ~self.df['is_neukd']]['kEUR'].sum()
                        val_new_sales = self.df[s_mask & self.df['is_neukd']]['kEUR'].sum()
                        
                        val_budget = self._get_budget_value(filter_val)
                        val_existing_budget = self._get_existing_budget_value(filter_val)
                        val_new_budget = self._get_new_budget_value(filter_val)
                        val_prior = self._get_prior_value(filter_val)
                        
                        rows.append({
                            'label': label,
                            'sales': val_sales,
                            'existing_sales': val_existing_sales,
                            'new_sales': val_new_sales,
                            'budget': val_budget,
                            'existing_budget': val_existing_budget,
                            'new_budget': val_new_budget,
                            'prior': val_prior,
                            'is_total': False,
                            'is_spacer': False
                        })
                        
                        sec_sales += val_sales
                        sec_budget += val_budget
                        sec_existing_budget += val_existing_budget
                        sec_new_budget += val_new_budget
                        sec_prior += val_prior
            else:
                # Fallback for sections with sub_region
                sub_region = section.get('sub_region')
                if sub_region:
                    sales_mask = (self.df['Sub Region'] == sub_region)
                    sec_sales = self.df[sales_mask]['kEUR'].sum()
                    sec_budget = self._get_budget_value(sub_region)
                    sec_existing_budget = self._get_existing_budget_value(sub_region)
                    sec_new_budget = self._get_new_budget_value(sub_region)
                    sec_prior = 0
                    
                    rows.append({
                        'label': section['title'],
                        'sales': sec_sales,
                        'existing_sales': 0.0,
                        'new_sales': 0.0,
                        'budget': sec_budget,
                        'existing_budget': sec_existing_budget,
                        'new_budget': sec_new_budget,
                        'prior': sec_prior,
                        'is_total': False,
                        'is_spacer': False
                    })
            
            # Add rows to report
            report_data.extend(rows)
            
            # Calculate section totals from rows
            sec_existing_sales = sum(r.get('existing_sales', 0) for r in rows if not r.get('is_spacer'))
            sec_new_sales = sum(r.get('new_sales', 0) for r in rows if not r.get('is_spacer'))
            sec_existing_budget = sum(r.get('existing_budget', 0) for r in rows if not r.get('is_spacer'))
            sec_new_budget = sum(r.get('new_budget', 0) for r in rows if not r.get('is_spacer'))
            
            # Add Section Total if requested
            if section.get('show_total'):
                report_data.append({
                    'label': section['title'],
                    'sales': sec_sales,
                    'existing_sales': sec_existing_sales,
                    'new_sales': sec_new_sales,
                    'budget': sec_budget,
                    'existing_budget': sec_existing_budget,
                    'new_budget': sec_new_budget,
                    'prior': sec_prior,
                    'is_total': True,
                    'is_spacer': False
                })
                
            # Store for aggregation
            section_totals[section['title']] = {
                'sales': sec_sales,
                'existing_sales': sec_existing_sales,
                'new_sales': sec_new_sales,
                'budget': sec_budget,
                'existing_budget': sec_existing_budget,
                'new_budget': sec_new_budget,
                'prior': sec_prior
            }
            
            # Add spacer with consistent schema
            report_data.append({
                'label': '',
                'sales': 0.0,
                'existing_sales': 0.0,
                'new_sales': 0.0,
                'budget': 0.0,
                'existing_budget': 0.0,
                'new_budget': 0.0,
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
        df['existing_sales'] = df['existing_sales'].astype(float)
        df['new_sales'] = df['new_sales'].astype(float)
        df['budget'] = df['budget'].astype(float)
        df['existing_budget'] = df['existing_budget'].astype(float)
        df['new_budget'] = df['new_budget'].astype(float)
        df['prior'] = df['prior'].astype(float)
        df['label'] = df['label'].astype(str)
        
        return df

    def render_report(self, df):
        # Print Header
        now = self.now
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        col_curr = f"{month_name}-{year_short}A MTD"
        
        print(f"{'kEUR':<30} {'Total Sales':>15} {'Existing':>12} {'New':>12} {'Total Budget':>14} {'Existing Budget':>16} {'New Budget':>12} {'Prior YoY':>12} {'% vs Budget':>12}")
        print("-" * 157)
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                print()
                continue
                
            label = row['label']
            sales = row['sales']
            existing_sales = row['existing_sales']
            new_sales = row['new_sales']
            budget = row['budget']
            existing_budget = row['existing_budget']
            new_budget = row['new_budget']
            prior = row['prior']
            
            # Add extra space above Company Sales totals
            if row.get('is_total') and 'Sales' in label:
                print()
            
            # Calculate %
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            # Format
            def _fmt(v):
                if abs(v) >= 1:    return f"{int(round(v))}"
                if 0 < abs(v) < 1: return f"{v:.1f}"
                if v == 0:         return "-"
                return "0"

            s_str    = _fmt(sales)
            ex_str   = _fmt(existing_sales)
            new_str  = _fmt(new_sales)
            b_str    = _fmt(budget)
            exb_str  = _fmt(existing_budget)
            newb_str = _fmt(new_budget)
            p_str    = _fmt(prior)
            pct_str  = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            print(f"{label:<30} {s_str:>10} {ex_str:>12} {new_str:>12} {b_str:>14} {exb_str:>16} {newb_str:>12} {p_str:>12} {pct_str:>12}")
            
            if row.get('is_total') or row.get('is_grand_total'):
                print("-" * 157)
    
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
        print("Starting Core Markets Report Generation with SharePoint Data")
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
                'prior': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_{prior_year}_gvl.csv',
                'py_mapping': f'/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/py25_regional_mappings.csv'
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
                        elif key == 'py_mapping':
                            local_paths[key] = str(project_root / 'data/inputs/mappings/py25_regional_mappings.csv')
            finally:
                sys.stdout = original_stdout  # Restore stdout
            
            current_step += 1
            print_progress(current_step, total_steps, "Applying entity mappings...")
            mapping_df = pd.read_csv(local_paths['mapping'])
            mapped_df = apply_mappings(qry_df, mapping_df)
            
            current_step += 1
            print_progress(current_step, total_steps, "Generating Core Markets report...")
            
            # Save mapped data to outputs folder for inspection
            current_year = get_current_year()
            output_dir = str(project_root / 'data/outputs')
            os.makedirs(output_dir, exist_ok=True)
            mapped_path = os.path.join(output_dir, f'qry_unified_mapped_{current_year}.csv')
            mapped_df.to_csv(mapped_path, index=False)
            
            # Determine path to sales split summary
            split_summary_path = str(project_root / 'data/outputs/sales_split_summary.csv')
            
            # Run the report generator with processed data
            generator = CoreMarketReportGenerator(
                str(project_root / 'src/config/core_market_report_structure.json'),
                mapped_path,
                local_paths['budget'],
                local_paths['prior'],
                split_summary_path,
                py_mapping_path=local_paths.get('py_mapping')
            )
            df = generator.calculate_report()
            print()  # Move to new line before report output
            generator.render_report(df)
            
            # Generate timestamped filename
            now = datetime.datetime.now()
            timestamp = now.strftime('%Y%m%d_%H%M%S')
            current_year = get_current_year()
            base_filename = f'management_report_core_markets_{current_year}_{timestamp}'
            
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
        split_summary_path = str(project_root / 'data/outputs/sales_split_summary.csv')
        generator = CoreMarketReportGenerator(
            project_root / 'src/config/core_market_report_structure.json',
            project_root / f'data/outputs/qry_unified_mapped_{current_year}.csv',
            project_root / f'data/inputs/budget/budget_GVL_{current_year}.csv',
            project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_gvl.csv',
            split_summary_path
        )
        df = generator.calculate_report()
        generator.render_report(df)
        
        # Generate timestamped filename
        now = datetime.datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        current_year = get_current_year()
        output_path = project_root / f'data/outputs/management_report_core_markets_{current_year}_{timestamp}.csv'
        
        generator.export_report(df, output_path)
    
    end_time = datetime.datetime.now()
    print("\nGenerator runtime: {:.2f} seconds".format((end_time - start_time).total_seconds()))