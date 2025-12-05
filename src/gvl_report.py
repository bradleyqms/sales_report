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

class GVLReportGenerator:
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
        
        # Clean Sales Employee in budget and prior
        self.budget_df['Sales_Employee_Cleaned'] = self.budget_df['Sales Employee / Account'].fillna('').str.strip()
        self.prior_df['Sales_Employee_Cleaned'] = self.prior_df['Sales Employee / Account'].fillna('').str.strip()
        
        # Filter Budget for Current Month
        # Budget Date is DD/MM/YYYY
        self.budget_df['Date'] = pd.to_datetime(self.budget_df['Date'], format='%d/%m/%Y')
        self.budget_month = self.budget_df[self.budget_df['Date'].dt.month == self.current_month].copy()
        
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
            
            if 'items' in section:
                # Section with items (sales employees)
                for item in section['items']:
                    label = item['label']
                    filter_val = item.get('filter_value')
                    
                    if filter_val:
                        s_mask = (self.df['Sales_Employee_Cleaned'] == filter_val)
                        val_sales = self.df[s_mask]['kEUR'].sum()
                        # budget and prior commented out
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
            
            # Add rows to report
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
    
    def export_report(self, df, base_path):
        """Export the report in formatted text style to CSV/TXT, HTML for Outlook, and PDF."""
        # Define column widths for text format
        now = datetime.datetime.now()
        month_name = now.strftime('%b')
        year_short = str(now.year)[2:]
        col_curr = f"{month_name}-{year_short}A MTD"
        col_widths = [35, 15, 12, 12, 12]
        headers = ['kEUR', col_curr, 'Budget', 'Prior', '% vs Bud']
        
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
            budget = row['budget']
            prior = row['prior']
            
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            row_line = f"{label:<{col_widths[0]}}{s_str:>{col_widths[1]}}{b_str:>{col_widths[2]}}{p_str:>{col_widths[3]}}{pct_str:>{col_widths[4]}}"
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
        </tr>
        """
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                html_content += '<tr><td colspan="5" style="height: 10px;"></td></tr>\n'
                continue
                
            label = row['label']
            sales = row['sales']
            budget = row['budget']
            prior = row['prior']
            
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            # Highlight totals
            bg_color = '#e6f3ff' if row.get('is_total') or row.get('is_grand_total') else 'white'
            
            html_content += f"""
            <tr style="background-color: {bg_color};">
                <td style="padding: 8px;">{label}</td>
                <td style="padding: 8px; text-align: right;">{s_str}</td>
                <td style="padding: 8px; text-align: right;">{b_str}</td>
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
        csv_df[col_curr] = csv_df['sales'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Budget'] = csv_df['budget'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df['Prior'] = csv_df['prior'].apply(lambda x: f"{int(round(x))}" if abs(x) >= 0.5 else ("-" if x == 0 else "0"))
        csv_df = csv_df.rename(columns={'label': 'kEUR'})
        csv_df = csv_df[['kEUR', col_curr, 'Budget', 'Prior', '% vs Bud']]
        
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
        title = Paragraph(f"GVL Management Report (MTD: {date_range})", styles['Heading1'])
        
        # Prepare table data
        pdf_data = [headers]
        
        for _, row in df.iterrows():
            if 'is_spacer' in df.columns and row.get('is_spacer') == True:
                pdf_data.append(['', '', '', '', ''])  # Empty row for spacing
                continue
                
            label = row['label']
            sales = row['sales']
            budget = row['budget']
            prior = row['prior']
            
            pct = (sales / budget * 100) if budget and budget != 0 else 0
            
            s_str = f"{int(round(sales))}" if abs(sales) >= 0.5 else ("-" if sales == 0 else "0")
            b_str = f"{int(round(budget))}" if abs(budget) >= 0.5 else ("-" if budget == 0 else "0")
            p_str = f"{int(round(prior))}" if abs(prior) >= 0.5 else ("-" if prior == 0 else "0")
            pct_str = f"{pct:.1f}%" if budget and budget != 0 else "-"
            
            pdf_data.append([label, s_str, b_str, p_str, pct_str])
        
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
                            local_paths[key] = str(project_root / 'data/inputs/budget/budget_GVL_2025.csv')
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
            base_filename = f'management_report_gvl_2025_{timestamp}'
            
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
        generator = GVLReportGenerator(
            project_root / 'src/config/gvl_report_structure.json',
            project_root / 'data/outputs/qry_unified_mapped_2025.csv',
            project_root / 'data/inputs/budget/budget_2025_processed.csv',
            project_root / 'data/inputs/prior_years/prior_sales_2024_processed.csv'
        )
        df = generator.calculate_report()
        generator.render_report(df)
        
        # Generate timestamped filename
        now = datetime.datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        output_path = project_root / f'data/outputs/management_report_gvl_2025_{timestamp}.csv'
        
        generator.export_report(df, output_path)
    
    end_time = datetime.datetime.now()
    print("\nGenerator runtime: {:.2f} seconds".format((end_time - start_time).total_seconds()))