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

class USASpaReportGenerator:
    def __init__(self, config_path, sales_path, budget_path, prior_path):
        self.config = self._load_config(config_path)
        try:
            self.df = pd.read_csv(sales_path)
            self.budget_df = pd.read_csv(budget_path)
            self.prior_df = pd.read_csv(prior_path)
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
        # Dates (use dynamic calculation from utils)
        now = datetime.datetime.now()
        self.current_month = get_current_month()
        self.current_year = get_current_year()
        self.prior_year = get_prior_year()
        
        # Filter Sales to AR (for QRY data, Document Type is 'AR', not 'AR Invoice')
        self.df = self.df[self.df['Document Type'] == 'AR'].copy()
        
        # Filter to USA Spa
        self.df = self.df[(self.df['Market_Group'] == 'USA') & (self.df['Channel_Level'] == 'Spa')].copy()
        
        # Prefer USD values for USA Spa; fall back to EUR if USD not available
        # Normalize to a single k-value column used throughout the report: 'kVAL'
        if 'Value_kUSD' in self.df.columns:
            # already in kUSD
            self.df['kVAL'] = pd.to_numeric(self.df['Value_kUSD'], errors='coerce').fillna(0)
            self.unit = 'kUSD'
        elif 'Value_in_USD_converted' in self.df.columns:
            # convert to kUSD
            self.df['kVAL'] = pd.to_numeric(self.df['Value_in_USD_converted'], errors='coerce').fillna(0) / 1000
            self.unit = 'kUSD'
        else:
            # fallback to EUR behaviour (existing behavior)
            value_col = 'Value_in_EUR_converted' if 'Value_in_EUR_converted' in self.df.columns else 'Total Value (EUR)'
            self.df['kVAL'] = pd.to_numeric(self.df[value_col], errors='coerce').fillna(0) / 1000
            self.unit = 'kEUR'

        # Keep the original detected unit so we can convert later if needed
        self._original_unit = self.unit
        # Prefer local USA-specific budget/prior files (if present) over any provided file
        repo_root = Path(__file__).parent.parent
        local_budget_dir = repo_root / 'data' / 'inputs' / 'budget'
        if local_budget_dir.exists():
            candidates = sorted(local_budget_dir.glob('*.csv'), key=lambda p: p.name.lower())
            chosen = None
            for keyword in ('usa_spa', 'usa', 'spa'):
                for p in candidates:
                    if keyword in p.name.lower():
                        chosen = p
                        break
                if chosen:
                    break

            if chosen:
                try:
                    alt_budget = pd.read_csv(chosen)
                    logging.info(f"Preferring local budget file: {chosen.name}")
                    self.budget_df = alt_budget
                except Exception:
                    pass

        # Filter Budget for Current Month
        # Budget Date is DD/MM/YYYY
        self.budget_df['Date'] = pd.to_datetime(self.budget_df['Date'], format='%d/%m/%Y')
        self.budget_month = self.budget_df[self.budget_df['Date'].dt.month == self.current_month].copy()
        # If the provided budget file contains no rows for the USA/Spa regions we are reporting,
        # try to find a USA-specific budget file in the inputs folder and use that instead.
        try:
            sales_regions = set(self.df['Region'].dropna().unique())
        except Exception:
            sales_regions = set()

        if self.budget_month.shape[0] == 0 or not any(self.budget_month['Region'].isin(sales_regions)):
            # search for candidate budget files in data/inputs/budget
            repo_root = Path(__file__).parent.parent
            budget_dir = repo_root / 'data' / 'inputs' / 'budget'
            if budget_dir.exists():
                # prioritize files with 'usa_spa' then 'usa'
                candidates = sorted(budget_dir.glob('*.csv'), key=lambda p: p.name.lower())
                chosen = None
                for keyword in ('usa_spa', 'usa', 'spa'):
                    for p in candidates:
                        if keyword in p.name.lower():
                            chosen = p
                            break
                    if chosen:
                        break

                if chosen:
                    try:
                        alt_budget = pd.read_csv(chosen)
                        alt_budget['Date'] = pd.to_datetime(alt_budget['Date'], format='%d/%m/%Y', errors='coerce')
                        alt_budget_month = alt_budget[alt_budget['Date'].dt.month == self.current_month].copy()
                        if alt_budget_month.shape[0] > 0 and any(alt_budget_month['Region'].isin(sales_regions)):
                            logging.info(f"Using fallback budget file: {chosen.name}")
                            self.budget_df = alt_budget
                            self.budget_month = alt_budget_month
                    except Exception:
                        pass
        
        # Filter Prior for Same Month Last Year
        # Prior file may have Date in DD/MM/YYYY format — try to parse safely
        try:
            self.prior_df['Date'] = pd.to_datetime(self.prior_df['Date'], format='%d/%m/%Y')
            self.prior_month = self.prior_df[(self.prior_df['Date'].dt.year == self.prior_year) & (self.prior_df['Date'].dt.month == self.current_month)].copy()
        except Exception:
            # Fallback to original string-starts behaviour if parsing fails
            target_prior_date = f"{self.prior_year}-{self.current_month:02d}"
            self.prior_month = self.prior_df[self.prior_df['Date'].astype(str).str.startswith(target_prior_date)].copy()

        # Prefer local USA-specific prior files if present
        local_prior_dir = repo_root / 'data' / 'inputs' / 'prior_years'
        if local_prior_dir.exists():
            candidates = sorted(local_prior_dir.glob('*.csv'), key=lambda p: p.name.lower())
            chosen = None
            for keyword in ('usa_spa', 'usa', 'spa'):
                for p in candidates:
                    if keyword in p.name.lower():
                        chosen = p
                        break
                if chosen:
                    break

            if chosen:
                try:
                    alt_prior = pd.read_csv(chosen)
                    logging.info(f"Preferring local prior file: {chosen.name}")
                    self.prior_df = alt_prior
                except Exception:
                    pass

        # Pre-aggregate budget and prior by Region for quick lookups (support both kUSD and kEUR)
        def sum_numeric(df_section, col):
            # Robustly parse numeric columns that may contain thousands separators or quoted strings
            if col not in df_section.columns:
                return pd.Series(dtype=float)

            tmp = df_section[['Region', col]].copy()
            # Normalize values: convert NaN to empty string, strip spaces, remove commas and other thousands separators
            tmp[col] = tmp[col].astype(str).str.replace('\u00a0', '', regex=False).str.replace(' ', '', regex=False).str.replace(',', '', regex=False)
            # Replace empty-like strings with 0
            tmp[col] = tmp[col].replace({'nan': '', 'None': '', '': '0'})
            # Convert to numeric
            tmp[col] = pd.to_numeric(tmp[col], errors='coerce').fillna(0.0)
            # Group by Region and sum
            grouped = tmp.groupby('Region')[col].sum()
            return grouped

        self.budget_region_kusd = sum_numeric(self.budget_month, 'Value_kUSD')
        self.budget_region_keur = sum_numeric(self.budget_month, 'Value_kEUR')
        self.prior_region_kusd = sum_numeric(self.prior_month, 'Value_kUSD')
        self.prior_region_keur = sum_numeric(self.prior_month, 'Value_kEUR')

        # If any USD budget/prior values are present for the sales regions, force report unit to kUSD
        try:
            usd_budget_total = float(self.budget_region_kusd.sum()) if not self.budget_region_kusd.empty else 0.0
            usd_prior_total = float(self.prior_region_kusd.sum()) if not self.prior_region_kusd.empty else 0.0
        except Exception:
            usd_budget_total = 0.0
            usd_prior_total = 0.0

        if usd_budget_total != 0.0 or usd_prior_total != 0.0:
            logging.info("Detected USD budget/prior values — forcing report unit to kUSD")
            # Force label to kUSD
            self.unit = 'kUSD'
            # If sales were originally in kEUR, convert actuals to kUSD using an exchange rate
            if getattr(self, '_original_unit', None) == 'kEUR':
                try:
                    eur_to_usd = float(os.getenv('EUR_TO_USD', '1.07'))
                except Exception:
                    eur_to_usd = 1.07
                # Convert kVAL (kEUR) -> kUSD by multiplying by EUR->USD rate
                logging.info(f"Converting sales actuals from kEUR to kUSD using rate {eur_to_usd}")
                try:
                    self.df['kVAL'] = pd.to_numeric(self.df['kVAL'], errors='coerce').fillna(0.0) * eur_to_usd
                except Exception:
                    pass
        
    def calculate_report(self):
        report_data = []
        section_totals = {}
        grand_total = {'actual': 0, 'budget': 0, 'prior': 0, 'diff_budget': 0, 'pct_budget': 0, 'diff_prior': 0, 'pct_prior': 0}
        
        for section in self.config['sections']:
            if section.get('is_grand_total'):
                report_data.append({
                    'label': section['title'],
                    'actual': grand_total['actual'],
                    'budget': grand_total['budget'],
                    'prior': grand_total['prior'],
                    'diff_budget': grand_total['diff_budget'],
                    'pct_budget': grand_total['pct_budget'],
                    'diff_prior': grand_total['diff_prior'],
                    'pct_prior': grand_total['pct_prior'],
                    'is_total': True,
                    'is_grand_total': True,
                    'is_spacer': False
                })
                continue
                
            if section.get('is_unmapped'):
                continue
                
            if section.get('is_total'):
                # Sum of other sections (e.g. Company 1 Total)
                t_actual = t_budget = t_prior = t_diff_budget = t_pct_budget = t_diff_prior = t_pct_prior = 0
                for comp in section['items'] if 'items' in section else section.get('components', []):
                    if comp in section_totals:
                        t_actual += section_totals[comp]['actual']
                        t_budget += section_totals[comp]['budget']
                        t_prior += section_totals[comp]['prior']
                        t_diff_budget += section_totals[comp]['diff_budget']
                        t_pct_budget = (t_actual / t_budget * 100) - 100 if t_budget != 0 else 0  # Recalculate for total
                        t_diff_prior += section_totals[comp]['diff_prior']
                        t_pct_prior = (t_actual / t_prior * 100) - 100 if t_prior != 0 else 0  # Recalculate for total
                
                report_data.append({
                    'label': section['title'],
                    'actual': t_actual,
                    'budget': t_budget,
                    'prior': t_prior,
                    'diff_budget': t_diff_budget,
                    'pct_budget': t_pct_budget,
                    'diff_prior': t_diff_prior,
                    'pct_prior': t_pct_prior,
                    'is_total': True,
                    'is_spacer': False,
                    'is_grand_total': False
                })
                
                # Note: do NOT add company-level totals to the grand total here.
                # Grand total will instead be accumulated from base sections (items/regions)
                continue

            # Regular Section
            sec_actual = 0
            sec_budget = 0
            sec_prior = 0
            sec_diff_budget = 0
            sec_pct_budget = 0
            sec_diff_prior = 0
            sec_pct_prior = 0
            
            rows = []
            
            if 'items' in section:
                # Section with items (regions)
                for item in section['items']:
                    label = item['label']
                    filter_val = item.get('filter_value')
                    
                    if filter_val:
                        s_mask = (self.df['Region'] == filter_val)
                        val_actual = self.df[s_mask]['kVAL'].sum()
                        # Use precomputed region-level lookups; prefer USD values when present, otherwise fall back to EUR
                        val_budget = 0
                        val_prior = 0
                        if filter_val in self.budget_region_kusd.index and self.budget_region_kusd.get(filter_val, 0) != 0:
                            val_budget = float(self.budget_region_kusd.get(filter_val, 0))
                        elif filter_val in self.budget_region_keur.index and self.budget_region_keur.get(filter_val, 0) != 0:
                            val_budget = float(self.budget_region_keur.get(filter_val, 0))

                        if filter_val in self.prior_region_kusd.index and self.prior_region_kusd.get(filter_val, 0) != 0:
                            val_prior = float(self.prior_region_kusd.get(filter_val, 0))
                        elif filter_val in self.prior_region_keur.index and self.prior_region_keur.get(filter_val, 0) != 0:
                            val_prior = float(self.prior_region_keur.get(filter_val, 0))
                        
                        val_diff_budget = val_actual - val_budget
                        val_pct_budget = (val_actual / val_budget * 100) - 100 if val_budget != 0 else 0
                        val_diff_prior = val_actual - val_prior
                        val_pct_prior = (val_actual / val_prior * 100) - 100 if val_prior != 0 else 0
                        
                        # Skip rows with no values across actual, budget and prior
                        if not (val_actual == 0 and val_budget == 0 and val_prior == 0):
                            rows.append({
                                'label': label,
                                'actual': val_actual,
                                'budget': val_budget,
                                'prior': val_prior,
                                'diff_budget': val_diff_budget,
                                'pct_budget': val_pct_budget,
                                'diff_prior': val_diff_prior,
                                'pct_prior': val_pct_prior,
                                'is_total': False,
                                'is_spacer': False
                            })
                        
                        sec_actual += val_actual
                        sec_budget += val_budget
                        sec_prior += val_prior
                        sec_diff_budget += val_diff_budget
                        sec_pct_budget = (sec_actual / sec_budget * 100) - 100 if sec_budget != 0 else 0
                        sec_diff_prior += val_diff_prior
                        sec_pct_prior = (sec_actual / sec_prior * 100) - 100 if sec_prior != 0 else 0
            else:
                # Fallback for sections with region
                region = section.get('region')
                if region:
                    sales_mask = (self.df['Region'] == region)
                    sec_actual = self.df[sales_mask]['kVAL'].sum()
                    # Look up aggregated budget/prior values by region preferring USD then EUR
                    sec_budget = float(self.budget_region_kusd.get(region, 0)) if region in self.budget_region_kusd.index and self.budget_region_kusd.get(region, 0) != 0 else float(self.budget_region_keur.get(region, 0)) if region in self.budget_region_keur.index else 0
                    sec_prior = float(self.prior_region_kusd.get(region, 0)) if region in self.prior_region_kusd.index and self.prior_region_kusd.get(region, 0) != 0 else float(self.prior_region_keur.get(region, 0)) if region in self.prior_region_keur.index else 0
                
                    sec_diff_budget = sec_actual - sec_budget
                    sec_pct_budget = (sec_actual / sec_budget * 100) - 100 if sec_budget != 0 else 0
                    sec_diff_prior = sec_actual - sec_prior
                    sec_pct_prior = (sec_actual / sec_prior * 100) - 100 if sec_prior != 0 else 0
                    
                    # Skip section row if there are no values
                    if not (sec_actual == 0 and sec_budget == 0 and sec_prior == 0):
                        rows.append({
                            'label': section['title'],
                            'actual': sec_actual,
                            'budget': sec_budget,
                            'prior': sec_prior,
                            'diff_budget': sec_diff_budget,
                            'pct_budget': sec_pct_budget,
                            'diff_prior': sec_diff_prior,
                            'pct_prior': sec_pct_prior,
                            'is_total': False
                        })
            
            # Add rows to report
            report_data.extend(rows)
            
            # Add Section Total if requested
            if section.get('show_total'):
                report_data.append({
                    'label': section['title'],
                    'actual': sec_actual,
                    'budget': sec_budget,
                    'prior': sec_prior,
                    'diff_budget': sec_diff_budget,
                    'pct_budget': sec_pct_budget,
                    'diff_prior': sec_diff_prior,
                    'pct_prior': sec_pct_prior,
                    'is_total': True,
                    'is_spacer': False
                })
                
            # Store for aggregation
            section_totals[section['title']] = {
                'actual': sec_actual,
                'budget': sec_budget,
                'prior': sec_prior,
                'diff_budget': sec_diff_budget,
                'pct_budget': sec_pct_budget,
                'diff_prior': sec_diff_prior,
                'pct_prior': sec_pct_prior
            }
            # Accumulate grand total from base sections only (exclude company-level 'is_total' sections)
            grand_total['actual'] += sec_actual
            grand_total['budget'] += sec_budget
            grand_total['prior'] += sec_prior
            grand_total['diff_budget'] += sec_diff_budget
            grand_total['pct_budget'] = (grand_total['actual'] / grand_total['budget'] * 100) - 100 if grand_total['budget'] != 0 else 0
            grand_total['diff_prior'] += sec_diff_prior
            grand_total['pct_prior'] = (grand_total['actual'] / grand_total['prior'] * 100) - 100 if grand_total['prior'] != 0 else 0
            
            # Add spacer with consistent schema
            report_data.append({
                'label': '',
                'actual': 0.0,
                'budget': 0.0,
                'prior': 0.0,
                'diff_budget': 0.0,
                'pct_budget': 0.0,
                'diff_prior': 0.0,
                'pct_prior': 0.0,
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
        
        for col in ['actual', 'budget', 'prior', 'diff_budget', 'pct_budget', 'diff_prior', 'pct_prior']:
            df[col] = df[col].astype(float)
        
        df['label'] = df['label'].astype(str)
        
        return df

    def render_report(self, df):
        # Print Header
        now = datetime.datetime.now()
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        col_curr = f"{month_name}-{year_short}A MTD"
        col_budget = f"{month_name}-{year_short}B"
        col_prior = f"{month_name}-{self.prior_year}A"
        
        print(f"USA Spa Report (Month-to-Date: {now.strftime('%B 1-%d, %Y')})")
        print(f"{self.unit:<30} {col_curr:>14} {col_budget:>10} {'25A vs 25B':>12} {'% 25A vs 25B':>14} {col_prior:>10} {'% 25A vs 24A':>14}")
        print("-" * 114)
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                print()
                continue
                
            label = row['label']
            actual = row['actual']
            budget = row['budget']
            prior = row['prior']
            diff_budget = row['diff_budget']
            pct_budget = row['pct_budget']
            diff_prior = row['diff_prior']
            pct_prior = row['pct_prior']
            
            # Add extra space above Company Sales totals
            if row.get('is_total') and 'Sales' in label:
                print()
            
            # Format
            a_str = f"{int(round(actual))}" if abs(actual) >= 0.5 else ("-" if actual == 0 else "0")
            b_str = f"{int(round(budget/1000))}" if abs(budget) >= 500 else ("-" if budget == 0 else "0")
            db_str = f"{int(round(diff_budget))}" if abs(diff_budget) >= 0.5 else ("-" if diff_budget == 0 else "0")
            pb_str = f"{pct_budget:.1f}%" if budget != 0 else "-"
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pp_str = f"{pct_prior:.1f}%" if prior != 0 else "-"
            
            print(f"{label:<30} {a_str:>14} {b_str:>10} {db_str:>12} {pb_str:>14} {p_str:>10} {pp_str:>14}")
            
            if row.get('is_total') or row.get('is_grand_total'):
                print("-" * 114)
    
    def export_report(self, df, base_path):
        """Export the report in formatted text style to CSV/TXT, HTML for Outlook, and PDF."""
        now = datetime.datetime.now()
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        # Define column widths for text format
        col_widths = [35, 16, 12, 12, 14, 12, 14]
        headers = [self.unit, f'{month_name}-{year_short}A MTD', f'{month_name}-{year_short}B', '25A vs 25B', '% 25A vs 25B', f'{month_name}-{self.prior_year}A', '% 25A vs 24A']
        
        # Create text format
        header_line = ''.join(f"{h:<{w}}" for h, w in zip(headers, col_widths))
        separator = '-' * len(header_line)
        
        formatted_lines = [header_line, separator]
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                formatted_lines.append('')
                continue
                
            label = row['label']
            actual = row['actual']
            budget = row['budget']
            prior = row['prior']
            diff_budget = row['diff_budget']
            pct_budget = row['pct_budget']
            diff_prior = row['diff_prior']
            pct_prior = row['pct_prior']
            
            a_str = f"{int(round(actual))}" if abs(actual) >= 0.5 else ("-" if actual == 0 else "0")
            b_str = f"{int(round(budget/1000))}" if abs(budget) >= 500 else ("-" if budget == 0 else "0")
            db_str = f"{int(round(diff_budget))}" if abs(diff_budget) >= 0.5 else ("-" if diff_budget == 0 else "0")
            pb_str = f"{pct_budget:.1f}%" if budget != 0 else "-"
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pp_str = f"{pct_prior:.1f}%" if prior != 0 else "-"
            
            row_line = f"{label:<{col_widths[0]}}{a_str:>{col_widths[1]}}{b_str:>{col_widths[2]}}{db_str:>{col_widths[3]}}{pb_str:>{col_widths[4]}}{p_str:>{col_widths[5]}}{pp_str:>{col_widths[6]}}"
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
        </tr>
        """
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                html_content += '<tr><td colspan="7" style="height: 10px;"></td></tr>\n'
                continue
                
            label = row['label']
            actual = row['actual']
            budget = row['budget']
            prior = row['prior']
            diff_budget = row['diff_budget']
            pct_budget = row['pct_budget']
            diff_prior = row['diff_prior']
            pct_prior = row['pct_prior']
            
            a_str = f"{int(round(actual))}" if abs(actual) >= 0.5 else ("-" if actual == 0 else "0")
            b_str = f"{int(round(budget/1000))}" if abs(budget) >= 500 else ("-" if budget == 0 else "0")
            db_str = f"{int(round(diff_budget))}" if abs(diff_budget) >= 0.5 else ("-" if diff_budget == 0 else "0")
            pb_str = f"{pct_budget:.1f}%" if budget != 0 else "-"
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pp_str = f"{pct_prior:.1f}%" if prior != 0 else "-"
            
            # Highlight totals
            bg_color = '#e6f3ff' if row.get('is_total') or row.get('is_grand_total') else 'white'
            
            html_content += f"""
            <tr style="background-color: {bg_color};">
                <td style="padding: 8px;">{label}</td>
                <td style="padding: 8px; text-align: right;">{a_str}</td>
                <td style="padding: 8px; text-align: right;">{b_str}</td>
                <td style="padding: 8px; text-align: right;">{db_str}</td>
                <td style="padding: 8px; text-align: right;">{pb_str}</td>
                <td style="padding: 8px; text-align: right;">{p_str}</td>
                <td style="padding: 8px; text-align: right;">{pp_str}</td>
            </tr>
            """
        
        html_content += "</table></body></html>"
        
        # Create proper CSV format with comma separators
        csv_df = df.copy()
        # Filter out spacer rows for CSV
        if 'is_spacer' in csv_df.columns:
            csv_df = csv_df[~csv_df['is_spacer'].fillna(False)]
        csv_df['% 25A vs 25B'] = csv_df.apply(lambda row: f"{row['pct_budget']:.1f}%" if row['budget'] != 0 else "-", axis=1)
        csv_df['% 25A vs 24A'] = csv_df.apply(lambda row: f"{row['pct_prior']:.1f}%" if row['prior'] != 0 else "-", axis=1)
        csv_df['Nov-25A'] = csv_df['actual'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Nov-25B'] = csv_df['budget'].apply(lambda x: f"{int(round(x/1000))}" if abs(x) >= 500 else ("-" if x == 0 else "0"))
        csv_df['25A vs 25B'] = csv_df['diff_budget'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Nov-24A'] = csv_df['prior'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        # Rename the label column to the appropriate unit (kUSD or kEUR) and select columns
        csv_df = csv_df.rename(columns={'label': self.unit})
        csv_df = csv_df[[self.unit, 'Nov-25A', 'Nov-25B', '25A vs 25B', '% 25A vs 25B', 'Nov-24A', '% 25A vs 24A']]
        
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
        
        # PDF title
        now_date = datetime.datetime.now()
        date_range = f"{now_date.strftime('%B')} 1-{now_date.day}, {now_date.year}"
        title = Paragraph(f"USA Spa Regional Report (MTD: {date_range})", styles['Heading1'])
        
        # Prepare table data
        pdf_data = [[self.unit, f'{month_name}-{year_short}A MTD', f'{month_name}-{year_short}B', '25A vs 25B', '% 25A vs 25B', f'{month_name}-{self.prior_year}A', '% 25A vs 24A']]
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                pdf_data.append(['', '', '', '', '', '', ''])  # Empty row for spacing
                continue
                
            label = row['label']
            actual = row['actual']
            budget = row['budget']
            prior = row['prior']
            diff_budget = row['diff_budget']
            pct_budget = row['pct_budget']
            diff_prior = row['diff_prior']
            pct_prior = row['pct_prior']
            
            a_str = f"{int(round(actual))}" if abs(actual) >= 0.5 else ("-" if actual == 0 else "0")
            b_str = f"{int(round(budget/1000))}" if abs(budget) >= 500 else ("-" if budget == 0 else "0")
            db_str = f"{int(round(diff_budget))}" if abs(diff_budget) >= 0.5 else ("-" if diff_budget == 0 else "0")
            pb_str = f"{pct_budget:.1f}%" if budget != 0 else "-"
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pp_str = f"{pct_prior:.1f}%" if prior != 0 else "-"
            
            pdf_data.append([label, a_str, b_str, db_str, pb_str, p_str, pp_str])
        
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
        print("Starting USA Spa Report Generation with SharePoint Data")
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
            other_paths = {
                'mapping': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/entity_mappings.csv',
                'budget': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/budget_2025_processed.csv',
                'prior': '/sites/DATAANDREPORTING/Shared Documents/SAP Extracts/prior_sales_2024_processed.csv'
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
                        elif key == 'budget':
                            local_paths[key] = str(project_root / 'data/inputs/budget/budget_2025_processed.csv')
                        elif key == 'prior':
                            local_paths[key] = str(project_root / 'data/inputs/prior_years/prior_sales_2024_processed.csv')
            finally:
                sys.stdout = original_stdout  # Restore stdout
            
            current_step += 1
            print_progress(current_step, total_steps, "Applying entity mappings...")
            mapping_df = pd.read_csv(local_paths['mapping'])
            mapped_df = apply_mappings(qry_df, mapping_df)
            
            current_step += 1
            print_progress(current_step, total_steps, "Generating management report...")
            
            # Save mapped data locally for reference/debugging
            mapped_path = os.path.join(temp_dir, 'qry_unified_mapped_2025.csv')
            mapped_df.to_csv(mapped_path, index=False)
            
            # Run the report generator with processed data
            generator = USASpaReportGenerator(
                str(project_root / 'src/config/usa_spa_report_structure.json'),
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
            base_filename = f'management_report_usa_spa_2025_{timestamp}'
            
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
        generator = USASpaReportGenerator(
            project_root / 'src/config/usa_spa_report_structure.json',
            project_root / 'data/outputs/qry_unified_mapped_2025.csv',
            project_root / 'data/inputs/budget/budget_2025_processed.csv',
            project_root / 'data/inputs/prior_years/prior_sales_2024_processed.csv'
        )
        df = generator.calculate_report()
        generator.render_report(df)
        
        # Generate timestamped filename
        now = datetime.datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        output_path = project_root / f'data/outputs/management_report_usa_spa_2025_{timestamp}.csv'
        
        generator.export_report(df, output_path)
    
    end_time = datetime.datetime.now()
    print("\nGenerator runtime: {:.2f} seconds".format((end_time - start_time).total_seconds()))