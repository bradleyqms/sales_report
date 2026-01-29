import pandas as pd
import json
import datetime
import os
import tempfile
import sys
import time
import logging
from pathlib import Path
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

class CoreMarketReportGenerator:
    def __init__(self, config_path, sales_path, budget_path, prior_path, split_summary_path=None):
        self.config = self._load_config(config_path)
        try:
            self.df = pd.read_csv(sales_path)
            self.budget_df = pd.read_csv(budget_path)
            self.prior_df = pd.read_csv(prior_path)
            # Load sales split summary if available
            self.split_summary = None
            if split_summary_path and os.path.exists(split_summary_path):
                try:
                    self.split_summary = pd.read_csv(split_summary_path)
                    logging.info(f"Loaded sales split summary from {split_summary_path}")
                except Exception as e:
                    logging.warning(f"Could not load sales split summary: {e}")
        except FileNotFoundError as e:
            logging.error(f"Required data file not found: {e}")
            raise
        except pd.errors.EmptyDataError as e:
            logging.error(f"Data file is empty: {e}")
            raise
        
        self._prepare_data()
        
    def _load_config(self, path):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Config file not found: {path}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file: {e}")
            raise
            
    def _prepare_data(self):
        # Dates
        now = datetime.datetime.now()
        self.current_month = now.month
        self.current_year = now.year
        self.prior_year = now.year - 1
        
        # Filter Sales to AR (for QRY data, Document Type is 'AR', not 'AR Invoice')
        self.df = self.df[self.df['Document Type'] == 'AR'].copy()
        
        # Convert Sales to kEUR
        value_col = 'Value_in_EUR_converted' if 'Value_in_EUR_converted' in self.df.columns else 'Total Value (EUR)'
        self.df['kEUR'] = self.df[value_col].fillna(0) / 1000
        
        # Clean Sub_Region in budget and prior - handle both 'Sub Region' and 'Subchannel / Partner' columns
        if 'Sub Region' in self.budget_df.columns:
            self.budget_df['Sub_Region_Cleaned'] = self.budget_df['Sub Region'].fillna('').str.strip()
        elif 'Subchannel / Partner' in self.budget_df.columns:
            self.budget_df['Sub_Region_Cleaned'] = self.budget_df['Subchannel / Partner'].fillna('').str.strip()
        else:
            self.budget_df['Sub_Region_Cleaned'] = ''
            
        if 'Sub Region' in self.prior_df.columns:
            self.prior_df['Sub_Region_Cleaned'] = self.prior_df['Sub Region'].fillna('').str.strip()
        elif 'Subchannel / Partner' in self.prior_df.columns:
            self.prior_df['Sub_Region_Cleaned'] = self.prior_df['Subchannel / Partner'].fillna('').str.strip()
        else:
            self.prior_df['Sub_Region_Cleaned'] = ''
        
        # Filter Budget for Current Month
        # Budget Date is DD/MM/YYYY
        self.budget_df['Date'] = pd.to_datetime(self.budget_df['Date'], format='%d/%m/%Y')
        self.budget_month = self.budget_df[self.budget_df['Date'].dt.month == self.current_month].copy()
        
        # Ensure budget numeric columns are parsed correctly
        if 'Value_kEUR' in self.budget_month.columns:
            self.budget_month['Value_kEUR'] = pd.to_numeric(self.budget_month['Value_kEUR'], errors='coerce').fillna(0)
        
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
        grand_total = {'Sales': 0, 'Existing_Sales': 0, 'New_Sales': 0, 'Budget': 0, 'Prior': 0}
        
        for section in self.config['sections']:
            if section.get('is_grand_total'):
                report_data.append({
                    'label': section['title'],
                    'sales': grand_total['Sales'],
                    'existing_sales': grand_total['Existing_Sales'],
                    'new_sales': grand_total['New_Sales'],
                    'budget': grand_total['Budget'],
                    'existing_budget': 0.0,
                    'new_budget': 0.0,
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
                t_sales = t_existing_sales = t_new_sales = t_budget = t_prior = 0
                for comp in section['items'] if 'items' in section else section.get('components', []):
                    if comp in section_totals:
                        t_sales += section_totals[comp]['sales']
                        t_existing_sales += section_totals[comp].get('existing_sales', 0)
                        t_new_sales += section_totals[comp].get('new_sales', 0)
                        t_budget += section_totals[comp]['budget']
                        t_prior += section_totals[comp]['prior']
                
                report_data.append({
                    'label': section['title'],
                    'sales': t_sales,
                    'existing_sales': t_existing_sales,
                    'new_sales': t_new_sales,
                    'budget': t_budget,
                    'existing_budget': 0.0,
                    'new_budget': 0.0,
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
                grand_total['Prior'] += t_prior
                continue

            # Regular Section
            sec_sales = 0
            sec_budget = 0
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
                        val_budget = self._get_budget_value(filter_val)
                        val_prior = self._get_prior_value(filter_val)
                        
                        # Get sales split by employee type (neukd vs regular)
                        entity = self.sub_region_to_entity.get(filter_val)
                        val_existing_sales = 0.0
                        val_new_sales = 0.0
                        
                        if entity and self.split_summary is not None:
                            # Get AR sales by type for this entity
                            # regular_ar = Existing customers, neukd_ar = New customer division
                            neukd_ar = self._get_sales_by_type(entity, 'AR', 'neukd')
                            regular_ar = self._get_sales_by_type(entity, 'AR', 'regular')
                            total_ar = neukd_ar + regular_ar
                            
                            # Calculate proportions and apply to actual sales
                            if total_ar > 0 and val_sales > 0:
                                existing_pct = regular_ar / total_ar  # regular = existing
                                new_pct = neukd_ar / total_ar  # neukd = new
                                # Use precise calculation and round at the end to avoid accumulation errors
                                val_existing_sales = round(val_sales * existing_pct, 1)
                                val_new_sales = round(val_sales - val_existing_sales, 1)  # Ensure sum equals total
                            elif val_sales > 0:
                                # If no split data, default to 100% existing
                                val_existing_sales = val_sales
                        else:
                            # Default: treat as existing sales if no entity mapping
                            val_existing_sales = val_sales
                        
                        rows.append({
                            'label': label,
                            'sales': val_sales,
                            'existing_sales': val_existing_sales,
                            'new_sales': val_new_sales,
                            'budget': val_budget,
                            'existing_budget': 0.0,
                            'new_budget': 0.0,
                            'prior': val_prior,
                            'is_total': False,
                            'is_spacer': False
                        })
                        
                        sec_sales += val_sales
                        sec_budget += val_budget
                        sec_prior += val_prior
            else:
                # Fallback for sections with sub_region
                sub_region = section.get('sub_region')
                if sub_region:
                    sales_mask = (self.df['Sub Region'] == sub_region)
                    sec_sales = self.df[sales_mask]['kEUR'].sum()
                    sec_budget = self._get_budget_value(sub_region)
                    sec_prior = 0
                    
                    rows.append({
                        'label': section['title'],
                        'sales': sec_sales,
                        'existing_sales': 0.0,
                        'new_sales': 0.0,
                        'budget': sec_budget,
                        'existing_budget': 0.0,
                        'new_budget': 0.0,
                        'prior': sec_prior,
                        'is_total': False,
                        'is_spacer': False
                    })
            
            # Add rows to report
            report_data.extend(rows)
            
            # Calculate section totals from rows
            sec_existing_sales = sum(r.get('existing_sales', 0) for r in rows if not r.get('is_spacer'))
            sec_new_sales = sum(r.get('new_sales', 0) for r in rows if not r.get('is_spacer'))
            
            # Add Section Total if requested
            if section.get('show_total'):
                report_data.append({
                    'label': section['title'],
                    'sales': sec_sales,
                    'existing_sales': sec_existing_sales,
                    'new_sales': sec_new_sales,
                    'budget': sec_budget,
                    'existing_budget': 0.0,
                    'new_budget': 0.0,
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
        now = datetime.datetime.now()
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
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            ex_str = f"{int(round(existing_sales))}" if abs(existing_sales) >= 0.5 else ("-" if existing_sales == 0 else "0")
            new_str = f"{int(round(new_sales))}" if abs(new_sales) >= 0.5 else ("-" if new_sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            exb_str = f"{int(round(existing_budget))}" if abs(existing_budget) >= 0.5 else ("-" if existing_budget == 0 else "0")
            newb_str = f"{int(round(new_budget))}" if abs(new_budget) >= 0.5 else ("-" if new_budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            print(f"{label:<30} {s_str:>10} {ex_str:>12} {new_str:>12} {b_str:>14} {exb_str:>16} {newb_str:>12} {p_str:>12} {pct_str:>12}")
            
            if row.get('is_total') or row.get('is_grand_total'):
                print("-" * 157)
    
    def export_report(self, df, base_path):
        """Export the report in formatted text style to CSV/TXT, HTML for Outlook, and PDF."""
        # Define column widths for text format
        now = datetime.datetime.now()
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        col_curr = f"{month_name}-{year_short}A MTD"
        col_widths = [35, 15, 12, 12, 14, 16, 12, 12, 12]
        headers = ['kEUR', 'Total Sales', 'Existing', 'New', 'Total Budget', 'Existing Budget', 'New Budget', 'Prior YoY', '% vs Budget']
        
        # Create text format
        header_line = ''.join(f"{h:<{w}}" for h, w in zip(headers, col_widths))
        separator = '-' * len(header_line)
        
        formatted_lines = [header_line, separator]
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                formatted_lines.append('')
                continue
                
            label = row['label']
            sales = row['sales']
            existing_sales = row['existing_sales']
            new_sales = row['new_sales']
            budget = row['budget']
            existing_budget = row['existing_budget']
            new_budget = row['new_budget']
            prior = row['prior']
            
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            ex_str = f"{int(round(existing_sales))}" if abs(existing_sales) >= 0.5 else ("-" if existing_sales == 0 else "0")
            new_str = f"{int(round(new_sales))}" if abs(new_sales) >= 0.5 else ("-" if new_sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            exb_str = f"{int(round(existing_budget))}" if abs(existing_budget) >= 0.5 else ("-" if existing_budget == 0 else "0")
            newb_str = f"{int(round(new_budget))}" if abs(new_budget) >= 0.5 else ("-" if new_budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            row_line = f"{label:<{col_widths[0]}}{s_str:>{col_widths[1]}}{ex_str:>{col_widths[2]}}{new_str:>{col_widths[3]}}{b_str:>{col_widths[4]}}{exb_str:>{col_widths[5]}}{newb_str:>{col_widths[6]}}{p_str:>{col_widths[7]}}{pct_str:>{col_widths[8]}}"
            formatted_lines.append(row_line)
            
            if row.get('is_total') or row.get('is_grand_total'):
                formatted_lines.append(separator)
        
        text_content = '\n'.join(formatted_lines)
        
        # Create HTML format for Outlook
        html_content = f"""
        <html>
        <body>
        <table border="1" style="border-collapse: collapse; font-family: Arial, sans-serif; font-size: 12px;">
        <tr style="background-color: #f0f0f0;">
            <th style="padding: 8px; text-align: left;">{headers[0]}</th>
            <th style="padding: 8px; text-align: right;">{headers[1]}</th>
            <th style="padding: 8px; text-align: right;">{headers[2]}</th>
            <th style="padding: 8px; text-align: right;">{headers[3]}</th>
            <th style="padding: 8px; text-align: right;">{headers[4]}</th>
            <th style="padding: 8px; text-align: right;">{headers[5]}</th>
            <th style="padding: 8px; text-align: right;">{headers[6]}</th>
            <th style="padding: 8px; text-align: right;">{headers[7]}</th>
            <th style="padding: 8px; text-align: right;">{headers[8]}</th>
        </tr>
        """
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                html_content += '<tr><td colspan="9" style="height: 10px;"></td></tr>\n'
                continue
                
            label = row['label']
            sales = row['sales']
            existing_sales = row['existing_sales']
            new_sales = row['new_sales']
            budget = row['budget']
            existing_budget = row['existing_budget']
            new_budget = row['new_budget']
            prior = row['prior']
            
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            ex_str = f"{int(round(existing_sales))}" if abs(existing_sales) >= 0.5 else ("-" if existing_sales == 0 else "0")
            new_str = f"{int(round(new_sales))}" if abs(new_sales) >= 0.5 else ("-" if new_sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            exb_str = f"{int(round(existing_budget))}" if abs(existing_budget) >= 0.5 else ("-" if existing_budget == 0 else "0")
            newb_str = f"{int(round(new_budget))}" if abs(new_budget) >= 0.5 else ("-" if new_budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            # Highlight totals
            bg_color = '#e6f3ff' if row.get('is_total') or row.get('is_grand_total') else 'white'
            
            html_content += f"""
            <tr style="background-color: {bg_color};">
                <td style="padding: 8px;">{label}</td>
                <td style="padding: 8px; text-align: right;">{s_str}</td>
                <td style="padding: 8px; text-align: right;">{ex_str}</td>
                <td style="padding: 8px; text-align: right;">{new_str}</td>
                <td style="padding: 8px; text-align: right;">{b_str}</td>
                <td style="padding: 8px; text-align: right;">{exb_str}</td>
                <td style="padding: 8px; text-align: right;">{newb_str}</td>
                <td style="padding: 8px; text-align: right;">{p_str}</td>
                <td style="padding: 8px; text-align: right;">{pct_str}</td>
            </tr>
            """
        
        html_content += "</table></body></html>"
        
        # Create proper CSV format with comma separators
        csv_df = df.copy()
        # Filter out spacer rows for CSV
        if 'is_spacer' in csv_df.columns:
            csv_df = csv_df[~csv_df['is_spacer'].fillna(False)]
        csv_df['% vs Bud'] = csv_df.apply(lambda row: f"{(row['sales'] / row['budget'] * 100):.1f}%" if row['budget'] and row['budget'] != 0 else "-", axis=1)
        csv_df['Total Sales'] = csv_df['sales'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Existing'] = csv_df['existing_sales'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['New'] = csv_df['new_sales'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Total Budget'] = csv_df['budget'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Existing Budget'] = csv_df['existing_budget'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['New Budget'] = csv_df['new_budget'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Prior YoY'] = csv_df['prior'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df = csv_df.rename(columns={'label': 'kEUR', '% vs Bud': '% vs Budget'})
        csv_df = csv_df[['kEUR', 'Total Sales', 'Existing', 'New', 'Total Budget', 'Existing Budget', 'New Budget', 'Prior YoY', '% vs Budget']]
        
        # Write to CSV file (proper CSV format with commas)
        csv_path = base_path
        csv_df.to_csv(csv_path, index=False, sep=',')
        print(f"Report exported to {csv_path}")
        
        # Write to TXT file (text format)
        txt_path = base_path.replace('.csv', '.txt')
        with open(txt_path, 'w') as f:
            f.write(text_content)
        print(f"Report exported to {txt_path}")
        
        # Write to HTML file (for Outlook)
        html_path = base_path.replace('.csv', '.html')
        with open(html_path, 'w') as f:
            f.write(html_content)
        print(f"Report exported to {html_path} (Outlook-ready HTML table)")
        
        # Create PDF format
        pdf_path = base_path.replace('.csv', '.pdf')
        doc = SimpleDocTemplate(pdf_path, pagesize=A4)
        styles = getSampleStyleSheet()
        
        # PDF title with MTD date range
        date_range = now.strftime('%B 1-%d, %Y')
        title = Paragraph(f"Core Markets Report (MTD: {date_range})", styles['Heading1'])
        
        # Prepare table data
        pdf_data = [headers]
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                pdf_data.append(['', '', '', '', '', '', '', '', ''])  # Empty row for spacing
                continue
                
            label = row['label']
            sales = row['sales']
            existing_sales = row['existing_sales']
            new_sales = row['new_sales']
            budget = row['budget']
            existing_budget = row['existing_budget']
            new_budget = row['new_budget']
            prior = row['prior']
            
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            ex_str = f"{int(round(existing_sales))}" if abs(existing_sales) >= 0.5 else ("-" if existing_sales == 0 else "0")
            new_str = f"{int(round(new_sales))}" if abs(new_sales) >= 0.5 else ("-" if new_sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            exb_str = f"{int(round(existing_budget))}" if abs(existing_budget) >= 0.5 else ("-" if existing_budget == 0 else "0")
            newb_str = f"{int(round(new_budget))}" if abs(new_budget) >= 0.5 else ("-" if new_budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            pdf_data.append([label, s_str, ex_str, new_str, b_str, exb_str, newb_str, p_str, pct_str])
        
        # Create table
        table = Table(pdf_data)
        
        # Style the table
        style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),  # Left align first column
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ])
        
        # Add special styling for totals
        row_idx = 1
        for _, row in df.iterrows():
            if row.get('is_total') or row.get('is_grand_total'):
                style.add('BACKGROUND', (0, row_idx), (-1, row_idx), colors.lightblue)
                style.add('FONTNAME', (0, row_idx), (-1, row_idx), 'Helvetica-Bold')
            row_idx += 1
        
        table.setStyle(style)
        
        # Build PDF
        elements = [title, Spacer(1, 20), table]
        doc.build(elements)
        print(f"Report exported to {pdf_path} (PDF format)")

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
                            local_paths[key] = str(project_root / f'data/inputs/budget/budget_GVL_{current_year}.csv')
                        elif key == 'prior':
                            local_paths[key] = str(project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv')
            finally:
                sys.stdout = original_stdout  # Restore stdout
            
            current_step += 1
            print_progress(current_step, total_steps, "Applying entity mappings...")
            mapping_df = pd.read_csv(local_paths['mapping'])
            mapped_df = apply_mappings(qry_df, mapping_df)
            
            current_step += 1
            print_progress(current_step, total_steps, "Generating Core Markets report...")
            
            # Save mapped data locally for reference/debugging
            current_year = get_current_year()
            mapped_path = os.path.join(temp_dir, f'qry_unified_mapped_{current_year}.csv')
            mapped_df.to_csv(mapped_path, index=False)
            
            # Determine path to sales split summary
            split_summary_path = str(project_root / 'data/outputs/sales_split_summary.csv')
            
            # Run the report generator with processed data
            generator = CoreMarketReportGenerator(
                str(project_root / 'src/config/core_market_report_structure.json'),
                mapped_path,
                local_paths['budget'],
                local_paths['prior'],
                split_summary_path
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
            project_root / f'data/inputs/prior_years/prior_sales_{prior_year}_processed.csv',
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