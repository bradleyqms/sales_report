"""
Unit tests for qry_data_mapping module.

Tests the apply_mappings() function to ensure:
- Known entities (customers and employees) map correctly
- Unknown entities are tracked and exported to CSV
- Edge cases are handled properly (empty dataframes, missing columns, None values)
"""

import pytest
import pandas as pd
import os
import tempfile
from pathlib import Path
import sys

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from qry_data_mapping import apply_mappings


@pytest.fixture
def sample_mapping_df():
    """Create a sample mapping DataFrame for testing."""
    return pd.DataFrame({
        'Sales_Employee': ['John Doe', 'Jane Smith', pd.NA, pd.NA],
        'Customer_Name': [pd.NA, pd.NA, 'ACME Corp', 'Beta Industries'],
        'Market_Group': ['Europe', 'USA', 'USA', 'Europe'],
        'Region': ['Germany', 'USA-East', 'USA-West', 'UK'],  # Changed Switzerland to UK to avoid AG-only filter
        'Channel_Level': ['Direct', 'Spa', 'Spa', 'Retail'],
        'Company_Group': ['Group A', 'Group B', 'Group C', 'Group D'],
        'Sales_Employee_Cleaned': ['John Doe', 'Jane Smith', 'ACME Rep', 'Beta Rep']
    })


@pytest.fixture
def sample_sales_df_employees():
    """Create sample sales data with employee mappings (GmbH/AG entities)."""
    return pd.DataFrame({
        'Sales Employee Name': ['John Doe', 'Jane Smith', 'Unknown Employee'],
        'Customer Name': ['Customer A', 'Customer B', 'Customer C'],
        'Company Entity': ['GmbH', 'AG', 'GmbH'],
        'Document Type': ['AR', 'AR', 'AR'],
        'Posting Date': ['2025-01-15', '2025-01-20', '2025-01-25'],
        'Total Value (EUR)': [1000, 2000, 1500]
    })


@pytest.fixture
def sample_sales_df_customers():
    """Create sample sales data with customer mappings (non-GmbH/AG entities)."""
    return pd.DataFrame({
        'Sales Employee Name': ['Rep A', 'Rep B', 'Rep C'],
        'Customer Name': ['ACME Corp', 'Beta Industries', 'Unknown Customer'],
        'Company Entity': ['Export', 'Export', 'Export'],
        'Document Type': ['AR', 'AR', 'AR'],
        'Posting Date': ['2025-02-01', '2025-02-05', '2025-02-10'],
        'Total Value (EUR)': [3000, 4000, 2500]
    })


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_apply_mappings_known_employees(sample_mapping_df, sample_sales_df_employees, temp_output_dir):
    """Test that known employees are mapped correctly."""
    result = apply_mappings(sample_sales_df_employees.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Check that John Doe is mapped
    john_row = result[result['Sales Employee Name'] == 'John Doe'].iloc[0]
    assert john_row['Market_Group'] == 'Europe'
    assert john_row['Region'] == 'Germany'
    assert john_row['Channel_Level'] == 'Direct'
    
    # Check that Jane Smith is mapped
    jane_row = result[result['Sales Employee Name'] == 'Jane Smith'].iloc[0]
    assert jane_row['Market_Group'] == 'USA'
    assert jane_row['Region'] == 'USA-East'
    assert jane_row['Channel_Level'] == 'Spa'


def test_apply_mappings_known_customers(sample_mapping_df, sample_sales_df_customers, temp_output_dir):
    """Test that known customers are mapped correctly."""
    result = apply_mappings(sample_sales_df_customers.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Check that ACME Corp is mapped
    acme_row = result[result['Customer Name'] == 'ACME Corp'].iloc[0]
    assert acme_row['Market_Group'] == 'USA'
    assert acme_row['Region'] == 'USA-West'
    assert acme_row['Channel_Level'] == 'Spa'
    
    # Check that Beta Industries is mapped
    beta_row = result[result['Customer Name'] == 'Beta Industries'].iloc[0]
    assert beta_row['Market_Group'] == 'Europe'
    assert beta_row['Region'] == 'UK'  # Changed from Switzerland to UK
    assert beta_row['Channel_Level'] == 'Retail'


def test_apply_mappings_unknown_entities_tracked(sample_mapping_df, sample_sales_df_employees, temp_output_dir):
    """Test that unknown entities are tracked and exported to CSV."""
    result = apply_mappings(sample_sales_df_employees.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Check that unmapped entities CSV was created
    unmapped_files = list(Path(temp_output_dir).glob('unmapped_entities_*.csv'))
    assert len(unmapped_files) == 1, "Unmapped entities CSV should be created"
    
    # Read the unmapped entities file
    unmapped_df = pd.read_csv(unmapped_files[0])
    
    # Check that unknown employee is tracked
    unknown_emp = unmapped_df[unmapped_df['entity_name'] == 'Unknown Employee']
    assert len(unknown_emp) == 1
    assert unknown_emp.iloc[0]['entity_type'] == 'employee'
    assert unknown_emp.iloc[0]['count'] == 1


def test_apply_mappings_unknown_customers_tracked(sample_mapping_df, sample_sales_df_customers, temp_output_dir):
    """Test that unknown customers are tracked and exported to CSV."""
    result = apply_mappings(sample_sales_df_customers.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Check that unmapped entities CSV was created
    unmapped_files = list(Path(temp_output_dir).glob('unmapped_entities_*.csv'))
    assert len(unmapped_files) == 1
    
    # Read the unmapped entities file
    unmapped_df = pd.read_csv(unmapped_files[0])
    
    # Check that unknown customer is tracked
    unknown_cust = unmapped_df[unmapped_df['entity_name'] == 'Unknown Customer']
    assert len(unknown_cust) == 1
    assert unknown_cust.iloc[0]['entity_type'] == 'customer'
    assert unknown_cust.iloc[0]['count'] == 1


def test_apply_mappings_date_tracking(sample_mapping_df, sample_sales_df_employees, temp_output_dir):
    """Test that first_seen and last_seen dates are tracked correctly."""
    result = apply_mappings(sample_sales_df_employees.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    unmapped_files = list(Path(temp_output_dir).glob('unmapped_entities_*.csv'))
    unmapped_df = pd.read_csv(unmapped_files[0])
    
    unknown_emp = unmapped_df[unmapped_df['entity_name'] == 'Unknown Employee'].iloc[0]
    assert unknown_emp['first_seen'] == '2025-01-25'
    assert unknown_emp['last_seen'] == '2025-01-25'


def test_apply_mappings_empty_dataframe(sample_mapping_df, temp_output_dir):
    """Test handling of empty sales DataFrame."""
    empty_sales = pd.DataFrame({
        'Sales Employee Name': [],
        'Customer Name': [],
        'Company Entity': [],
        'Document Type': [],
        'Posting Date': [],
        'Total Value (EUR)': []
    })
    
    result = apply_mappings(empty_sales, sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Should return empty DataFrame without errors
    assert len(result) == 0
    
    # No unmapped entities file should be created
    unmapped_files = list(Path(temp_output_dir).glob('unmapped_entities_*.csv'))
    assert len(unmapped_files) == 0


def test_apply_mappings_missing_columns(sample_mapping_df, temp_output_dir):
    """Test handling of sales DataFrame with missing expected columns."""
    incomplete_sales = pd.DataFrame({
        'Sales Employee Name': ['John Doe'],
        'Company Entity': ['GmbH'],
        # Missing: Customer Name, Document Type, etc.
    })
    
    # Should not raise an error, but may log warnings
    try:
        result = apply_mappings(incomplete_sales, sample_mapping_df.copy(), output_dir=temp_output_dir)
        # Function should complete without exception
        assert True
    except Exception as e:
        pytest.fail(f"apply_mappings raised exception with missing columns: {e}")


def test_apply_mappings_none_values(sample_mapping_df, temp_output_dir):
    """Test handling of None/NaN values in sales data."""
    sales_with_nones = pd.DataFrame({
        'Sales Employee Name': ['John Doe', None, pd.NA],
        'Customer Name': [None, 'ACME Corp', pd.NA],
        'Company Entity': ['GmbH', 'Export', 'Export'],
        'Document Type': ['AR', 'AR', 'AR'],
        'Posting Date': ['2025-01-15', None, pd.NA],
        'Total Value (EUR)': [1000, 2000, None]
    })
    
    result = apply_mappings(sales_with_nones.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Should handle None values gracefully
    assert len(result) >= 1  # At least some rows should remain
    
    # John Doe should still be mapped
    john_rows = result[result['Sales Employee Name'] == 'John Doe']
    if len(john_rows) > 0:
        assert john_rows.iloc[0]['Market_Group'] == 'Europe'


def test_apply_mappings_interco_filter(sample_mapping_df, temp_output_dir):
    """Test that Interco customers are filtered out."""
    sales_with_interco = pd.DataFrame({
        'Sales Employee Name': ['John Doe', 'Jane Smith'],
        'Customer Name': ['Normal Customer', 'Interco XYZ'],
        'Company Entity': ['Export', 'Export'],
        'Document Type': ['AR', 'AR'],
        'Posting Date': ['2025-01-15', '2025-01-16'],
        'Total Value (EUR)': [1000, 2000]
    })
    
    result = apply_mappings(sales_with_interco.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Interco customer should be filtered out
    assert 'Interco XYZ' not in result['Customer Name'].values
    assert 'Normal Customer' in result['Customer Name'].values


def test_apply_mappings_channel_level_replacement(sample_mapping_df, temp_output_dir):
    """Test that 'eCommerce (excl. USA)' is replaced with 'eCommerce EU (incl. UK)'."""
    # Add a mapping with old channel level
    mapping_with_old = sample_mapping_df.copy()
    mapping_with_old.loc[0, 'Channel_Level'] = 'eCommerce (excl. USA)'
    
    sales_for_channel = pd.DataFrame({
        'Sales Employee Name': ['John Doe'],
        'Customer Name': ['Customer A'],
        'Company Entity': ['GmbH'],
        'Document Type': ['AR'],
        'Posting Date': ['2025-01-15'],
        'Total Value (EUR)': [1000]
    })
    
    result = apply_mappings(sales_for_channel.copy(), mapping_with_old, output_dir=temp_output_dir)
    
    # Check that channel level was replaced
    assert result.iloc[0]['Channel_Level'] == 'eCommerce EU (incl. UK)'


def test_apply_mappings_export_ar_only(sample_mapping_df, temp_output_dir):
    """Test that non-AR document types are filtered for Export entity."""
    sales_with_mixed = pd.DataFrame({
        'Sales Employee Name': ['Rep A', 'Rep B', 'Rep C'],
        'Customer Name': ['Customer 1', 'Customer 2', 'Customer 3'],
        'Company Entity': ['Export', 'Export', 'GmbH'],
        'Document Type': ['AR', 'CN', 'CN'],  # CN should be filtered for Export
        'Posting Date': ['2025-01-15', '2025-01-16', '2025-01-17'],
        'Total Value (EUR)': [1000, 2000, 3000]
    })
    
    result = apply_mappings(sales_with_mixed.copy(), sample_mapping_df.copy(), output_dir=temp_output_dir)
    
    # Export with CN should be filtered out, GmbH with CN should remain
    export_rows = result[result['Company Entity'] == 'Export']
    assert all(export_rows['Document Type'] == 'AR')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
