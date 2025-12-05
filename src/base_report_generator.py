"""
Base report generator class with shared functionality.

This module provides an abstract base class that encapsulates common report
generation patterns including config loading, date preparation, PDF styling,
table formatting, and multi-format exports.
"""

from abc import ABC, abstractmethod
import json
import datetime
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

from utils import get_current_year, get_prior_year, get_current_month, format_mtd_date_range

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class BaseReportGenerator(ABC):
    """
    Abstract base class for report generators.
    
    Provides common functionality for:
    - Config file loading and validation
    - Date calculations (current year, prior year, current month)
    - PDF document styling and table formatting
    - Multi-format exports (CSV, TXT, HTML, PDF, XLSX)
    
    Subclasses must implement:
    - calculate_report(): Generate report data as DataFrame
    - render_report(df): Display report to console
    """
    
    def __init__(self, config_path: str, sales_path: str, budget_path: str, prior_path: str):
        """
        Initialize the report generator.
        
        Args:
            config_path: Path to JSON configuration file
            sales_path: Path to sales data CSV
            budget_path: Path to budget data CSV
            prior_path: Path to prior year data CSV
        """
        self.config = self._load_config(config_path)
        self._load_data_files(sales_path, budget_path, prior_path)
        self._prepare_dates()
    
    def _load_config(self, path: str) -> dict:
        """
        Load and validate JSON configuration file.
        
        Args:
            path: Path to config file
            
        Returns:
            Parsed configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Config file not found: {path}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file: {e}")
            raise
    
    def _load_data_files(self, sales_path: str, budget_path: str, prior_path: str) -> None:
        """
        Load CSV data files into DataFrames.
        
        Args:
            sales_path: Path to sales data CSV
            budget_path: Path to budget data CSV
            prior_path: Path to prior year data CSV
            
        Raises:
            FileNotFoundError: If any data file doesn't exist
            pd.errors.EmptyDataError: If any data file is empty
        """
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
    
    def _prepare_dates(self) -> None:
        """
        Calculate and store commonly used date values.
        
        Sets instance variables:
        - current_year: Current year (int)
        - prior_year: Prior year (int)
        - current_month: Current month (int, 1-12)
        - now: Current datetime object
        """
        self.now = datetime.datetime.now()
        self.current_year = get_current_year()
        self.prior_year = get_prior_year()
        self.current_month = get_current_month()
    
    def get_pdf_styles(self) -> Dict[str, TableStyle]:
        """
        Get standard PDF table styles.
        
        Returns:
            Dictionary of TableStyle objects for different table types:
            - 'header': Styling for header row
            - 'data': Styling for data rows
            - 'total': Styling for total rows
            - 'grand_total': Styling for grand total rows
        """
        styles = {
            'header': TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('ALIGN', (0, 1), (0, -1), 'LEFT'),  # Left align first column
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]),
            'data': TableStyle([
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ]),
            'total': TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ]),
            'grand_total': TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightsteelblue),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ])
        }
        return styles
    
    def create_pdf_table(self, data: List[List], title: str, pagesize=A4) -> SimpleDocTemplate:
        """
        Create a styled PDF table document.
        
        Args:
            data: 2D list of table data (rows x columns)
            title: Document title
            pagesize: ReportLab page size (default A4)
            
        Returns:
            Configured SimpleDocTemplate ready to build
        """
        # This method would be implemented by subclasses with specific table layouts
        pass
    
    def format_number(self, value: float, zero_placeholder: str = "-") -> str:
        """
        Format a numeric value for display.
        
        Args:
            value: Number to format
            zero_placeholder: String to display for zero values (default "-")
            
        Returns:
            Formatted string (rounded integer or placeholder)
            
        Example:
            >>> format_number(1234.56)
            '1235'
            >>> format_number(0.0)
            '-'
        """
        if abs(value) >= 0.5:
            return f"{int(round(value))}"
        elif value == 0:
            return zero_placeholder
        else:
            return "0"
    
    def format_percentage(self, numerator: float, denominator: float, 
                         zero_placeholder: str = "-") -> str:
        """
        Format a percentage value for display.
        
        Args:
            numerator: Numerator value
            denominator: Denominator value
            zero_placeholder: String to display when denominator is zero
            
        Returns:
            Formatted percentage string like "12.5%" or placeholder
            
        Example:
            >>> format_percentage(125, 100)
            '25.0%'
            >>> format_percentage(50, 0)
            '-'
        """
        if denominator and denominator != 0:
            pct = (numerator / denominator * 100) - 100
            return f"{pct:.1f}%"
        return zero_placeholder
    
    def export_to_csv(self, df: pd.DataFrame, path: str, headers: List[str]) -> None:
        """
        Export DataFrame to CSV with custom headers.
        
        Args:
            df: DataFrame to export
            path: Output file path
            headers: Column headers to use
        """
        output_df = df.copy()
        if len(headers) == len(output_df.columns):
            output_df.columns = headers
        output_df.to_csv(path, index=False, sep=',')
        logging.info(f"Report exported to {path}")
    
    def export_to_txt(self, content: str, path: str) -> None:
        """
        Export formatted text content to file.
        
        Args:
            content: Text content to write
            path: Output file path
        """
        with open(path, 'w') as f:
            f.write(content)
        logging.info(f"Report exported to {path}")
    
    def export_to_html(self, df: pd.DataFrame, path: str, headers: List[str], 
                      title: str = "Report") -> None:
        """
        Export DataFrame to HTML table for Outlook.
        
        Args:
            df: DataFrame to export
            path: Output file path
            headers: Column headers to use
            title: HTML page title
        """
        html_content = f"""
        <html>
        <head><title>{title}</title></head>
        <body>
        <h2>{title}</h2>
        <table border="1" style="border-collapse: collapse; font-family: Arial, sans-serif; font-size: 12px;">
        <tr style="background-color: #f0f0f0;">
        """
        
        for header in headers:
            align = "left" if headers.index(header) == 0 else "right"
            html_content += f'<th style="padding: 8px; text-align: {align};">{header}</th>'
        
        html_content += "</tr>\n"
        
        for _, row in df.iterrows():
            is_total = row.get('is_total', False) or row.get('is_grand_total', False)
            bg_color = '#e6f3ff' if is_total else 'white'
            html_content += f'<tr style="background-color: {bg_color};">\n'
            
            for col in df.columns:
                value = row[col]
                align = "left" if df.columns.get_loc(col) == 0 else "right"
                html_content += f'<td style="padding: 8px; text-align: {align};">{value}</td>'
            
            html_content += "</tr>\n"
        
        html_content += "</table></body></html>"
        
        with open(path, 'w') as f:
            f.write(html_content)
        logging.info(f"Report exported to {path}")
    
    @abstractmethod
    def calculate_report(self) -> pd.DataFrame:
        """
        Calculate report data.
        
        Must be implemented by subclasses to generate report-specific
        calculations and return a DataFrame with the report data.
        
        Returns:
            DataFrame containing calculated report data
        """
        pass
    
    @abstractmethod
    def render_report(self, df: pd.DataFrame) -> None:
        """
        Render report to console.
        
        Must be implemented by subclasses to display report data
        in a formatted table to stdout.
        
        Args:
            df: DataFrame containing report data from calculate_report()
        """
        pass
