"""
Shared utility functions for sales reporting system.

This module provides common utilities used across multiple report generators,
including progress display, date calculations, and formatting helpers.
"""

import sys
import datetime
from typing import Optional


def print_progress(current: int, total: int, message: str = "") -> None:
    """
    Print a simple progress bar to stdout.
    
    Args:
        current: Current step number (1-indexed)
        total: Total number of steps
        message: Optional message to display alongside progress bar
        
    Example:
        >>> print_progress(3, 10, "Processing data...")
        [#########---------------------] 30% Processing data...
    """
    percentage = int((current / total) * 100)
    bar_length = 30
    filled_length = int(bar_length * current // total)
    bar = '#' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f'\r[{bar}] {percentage}% {message}')
    sys.stdout.flush()
    if current == total:
        print()  # New line when complete


def get_current_year() -> int:
    """
    Get the current year dynamically.
    
    Returns:
        Current year as integer
        
    Example:
        >>> get_current_year()
        2025
    """
    return datetime.datetime.now().year


def get_prior_year() -> int:
    """
    Get the prior year (current year - 1) dynamically.
    
    Returns:
        Prior year as integer
        
    Example:
        >>> get_prior_year()
        2024
    """
    return datetime.datetime.now().year - 1


def get_current_month() -> int:
    """
    Get the current month dynamically.
    
    Returns:
        Current month as integer (1-12)
        
    Example:
        >>> get_current_month()
        12
    """
    return datetime.datetime.now().month


def format_mtd_date_range(now: Optional[datetime.datetime] = None) -> str:
    """
    Format a Month-to-Date (MTD) date range string.
    
    Args:
        now: Optional datetime object. If None, uses current datetime.
        
    Returns:
        Formatted string like "December 1-2, 2025"
        
    Example:
        >>> format_mtd_date_range()
        'December 1-2, 2025'
    """
    if now is None:
        now = datetime.datetime.now()
    return now.strftime('%B 1-%d, %Y')


def format_column_header(now: Optional[datetime.datetime] = None, include_mtd: bool = True) -> str:
    """
    Format a column header for current period actuals.
    
    Args:
        now: Optional datetime object. If None, uses current datetime.
        include_mtd: If True, appends " MTD" to the header
        
    Returns:
        Formatted string like "Dec-25A MTD" or "Dec-25A"
        
    Example:
        >>> format_column_header()
        'Dec-25A MTD'
    """
    if now is None:
        now = datetime.datetime.now()
    month_name = now.strftime('%b')
    year_short = str(now.year)[2:]
    header = f"{month_name}-{year_short}A"
    if include_mtd:
        header += " MTD"
    return header


def format_budget_header(now: Optional[datetime.datetime] = None) -> str:
    """
    Format a column header for current period budget.
    
    Args:
        now: Optional datetime object. If None, uses current datetime.
        
    Returns:
        Formatted string like "Dec-25B"
        
    Example:
        >>> format_budget_header()
        'Dec-25B'
    """
    if now is None:
        now = datetime.datetime.now()
    month_name = now.strftime('%b')
    year_short = str(now.year)[2:]
    return f"{month_name}-{year_short}B"


def format_prior_header(now: Optional[datetime.datetime] = None, prior_year: Optional[int] = None) -> str:
    """
    Format a column header for prior year actuals.
    
    Args:
        now: Optional datetime object. If None, uses current datetime.
        prior_year: Optional prior year. If None, calculates dynamically.
        
    Returns:
        Formatted string like "Dec-24A"
        
    Example:
        >>> format_prior_header()
        'Dec-24A'
    """
    if now is None:
        now = datetime.datetime.now()
    if prior_year is None:
        prior_year = get_prior_year()
    month_name = now.strftime('%b')
    year_short = str(prior_year)[2:]
    return f"{month_name}-{year_short}A"


def get_year_labels(now: Optional[datetime.datetime] = None) -> tuple[str, str]:
    """
    Get current and prior year labels for reports.
    
    Args:
        now: Optional datetime object. If None, uses current datetime.
        
    Returns:
        Tuple of (current_year_short, prior_year_short) like ('25', '24')
        
    Example:
        >>> get_year_labels()
        ('25', '24')
    """
    if now is None:
        now = datetime.datetime.now()
    current_year_short = str(now.year)[2:]
    prior_year_short = str(now.year - 1)[2:]
    return current_year_short, prior_year_short
