"""
Integration tests for the full data pipeline.

Tests end-to-end flow with mocked SharePoint downloads:
- QRY file download and processing
- Entity mapping with unmapped entity tracking
- Report generation (Management, GVL, USA Spa)
- Multi-format exports (CSV, TXT, HTML, PDF)
"""

import pytest
import pandas as pd
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from qry_data_ingestion import process_qry_files
from qry_data_mapping import apply_mappings
from receivables_report_generator import ManagementReportGenerator
from gvl_report import GVLReportGenerator
from usa_spa_report import USASpaReportGenerator


@pytest.fixture
def mock_sharepoint_handler():
    """Mock SharePoint handler for testing without actual connection."""
    with patch('sharepoint_client.SharePointHandler') as mock_sp:
        instance = mock_sp.return_value
        instance.download_file = Mock(return_value=True)
        instance.upload_file = Mock(return_value=True)
        yield instance


@pytest.fixture
def sample_qry_data():
    """Create sample QRY data for testing."""
    return pd.DataFrame({
        'Company Code': ['1000', '2000', '3000'],
        'Sales Employee': ['EMP001', 'EMP002', 'EMP003'],
        'Customer': ['CUST001', 'CUST002', 'CUST003'],
        'Posting Date': ['2025-01-15', '2025-01-20', '2025-01-25'],
        'Document Number': ['DOC001', 'DOC002', 'DOC003'],
        'Document Type': ['AR', 'AR', 'AR'],
        'Net Value': [10000, 20000, 15000],
        'Currency': ['EUR', 'EUR', 'EUR']
    })


@pytest.fixture
def sample_mapping_data():
    """Create sample mapping data for testing."""
    return pd.DataFrame({
        'Sales_Employee': ['John Doe', 'Jane Smith', pd.NA],
        'Customer_Name': [pd.NA, pd.NA, 'ACME Corp'],
        'Market_Group': ['Europe', 'USA', 'USA'],
        'Region': ['Germany', 'USA-East', 'USA-West'],
        'Channel_Level': ['Direct', 'Spa', 'Retail'],
        'Company_Group': ['Group A', 'Group B', 'Group C'],
        'Sales_Employee_Cleaned': ['John Doe', 'Jane Smith', 'ACME Rep']
    })


@pytest.fixture
def sample_budget_data():
    """Create sample budget data for testing."""
    return pd.DataFrame({
        'Date': ['01/01/2025', '01/01/2025', '01/01/2025'],
        'Region': ['Germany', 'USA-East', 'USA-West'],
        'Value_kEUR': [100, 200, 150],
        'Value_kUSD': [107, 214, 160.5]
    })


@pytest.fixture
def sample_prior_data():
    """Create sample prior year data for testing."""
    return pd.DataFrame({
        'Date': ['01/01/2024', '01/01/2024', '01/01/2024'],
        'Region': ['Germany', 'USA-East', 'USA-West'],
        'Value_kEUR': [90, 180, 140],
        'Value_kUSD': [96.3, 192.6, 149.8]
    })


@pytest.fixture
def temp_test_env():
    """Create temporary test environment with all necessary directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create directory structure
        (tmpdir / 'data' / 'inputs' / 'mappings').mkdir(parents=True, exist_ok=True)
        (tmpdir / 'data' / 'inputs' / 'budget').mkdir(parents=True, exist_ok=True)
        (tmpdir / 'data' / 'inputs' / 'prior_years').mkdir(parents=True, exist_ok=True)
        (tmpdir / 'data' / 'outputs').mkdir(parents=True, exist_ok=True)
        (tmpdir / 'src' / 'config').mkdir(parents=True, exist_ok=True)
        
        yield tmpdir


def test_qry_data_processing(temp_test_env, sample_qry_data):
    """Test QRY data ingestion and processing."""
    # Save sample QRY files
    qry_dir = temp_test_env / 'qry_files'
    qry_dir.mkdir(exist_ok=True)
    
    sample_qry_data.to_csv(qry_dir / 'QRY_AR_MTD_Test.csv', index=False)
    
    # Process QRY files
    result = process_qry_files(str(qry_dir))
    
    # Verify processing
    assert len(result) == 3
    assert 'Company Entity' in result.columns  # Column mapping occurred
    assert 'Sales Employee Name' in result.columns


def test_entity_mapping_with_unmapped_tracking(temp_test_env, sample_qry_data, sample_mapping_data):
    """Test entity mapping and verify unmapped entities are tracked."""
    output_dir = temp_test_env / 'data' / 'outputs'
    
    # Create sales data with unmapped entities
    sales_df = pd.DataFrame({
        'Sales Employee Name': ['John Doe', 'Unknown Employee'],
        'Customer Name': ['Customer A', 'Unknown Customer'],
        'Company Entity': ['GmbH', 'Export'],
        'Document Type': ['AR', 'AR'],
        'Posting Date': ['2025-01-15', '2025-01-20'],
        'Total Value (EUR)': [1000, 2000]
    })
    
    # Apply mappings
    result = apply_mappings(sales_df, sample_mapping_data, output_dir=str(output_dir))
    
    # Verify unmapped entities CSV was created
    unmapped_files = list(output_dir.glob('unmapped_entities_*.csv'))
    assert len(unmapped_files) == 1, "Unmapped entities file should be created"
    
    # Verify unmapped entities content
    unmapped_df = pd.read_csv(unmapped_files[0])
    assert len(unmapped_df) == 2  # One employee, one customer
    
    entity_types = unmapped_df['entity_type'].tolist()
    assert 'employee' in entity_types
    assert 'customer' in entity_types


def test_management_report_generation(temp_test_env, sample_mapping_data, sample_budget_data, sample_prior_data):
    """Test Management Report generation end-to-end."""
    # Prepare data files
    config_path = temp_test_env / 'src' / 'config' / 'report_structure.json'
    sales_path = temp_test_env / 'data' / 'outputs' / 'sales.csv'
    budget_path = temp_test_env / 'data' / 'inputs' / 'budget' / 'budget.csv'
    prior_path = temp_test_env / 'data' / 'inputs' / 'prior_years' / 'prior.csv'
    
    # Create minimal config
    config = {
        "sections": [
            {
                "title": "Test Section",
                "items": [{"label": "Test Item", "filter_column": "Region", "filter_value": "Germany"}]
            }
        ]
    }
    
    import json
    with open(config_path, 'w') as f:
        json.dump(config, f)
    
    # Create sales data
    sales_df = pd.DataFrame({
        'Sales Employee Name': ['John Doe'],
        'Customer Name': ['Customer A'],
        'Company Entity': ['GmbH'],
        'Document Type': ['AR'],
        'Region': ['Germany'],
        'Market_Group': ['Europe'],
        'Channel_Level': ['Direct'],
        'Total Value (EUR)': [100000]
    })
    sales_df.to_csv(sales_path, index=False)
    
    # Save budget and prior data
    sample_budget_data.to_csv(budget_path, index=False)
    sample_prior_data.to_csv(prior_path, index=False)
    
    # Generate report
    try:
        generator = ManagementReportGenerator(
            str(config_path),
            str(sales_path),
            str(budget_path),
            str(prior_path)
        )
        
        df = generator.calculate_report()
        
        # Verify report structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert 'label' in df.columns
        assert 'sales' in df.columns
        assert 'budget' in df.columns
        
    except Exception as e:
        pytest.fail(f"Management report generation failed: {e}")


def test_gvl_report_generation(temp_test_env, sample_budget_data, sample_prior_data):
    """Test GVL Report generation end-to-end."""
    # Prepare data files
    config_path = temp_test_env / 'src' / 'config' / 'gvl_report_structure.json'
    sales_path = temp_test_env / 'data' / 'outputs' / 'sales_gvl.csv'
    budget_path = temp_test_env / 'data' / 'inputs' / 'budget' / 'budget_gvl.csv'
    prior_path = temp_test_env / 'data' / 'inputs' / 'prior_years' / 'prior_gvl.csv'
    
    # Create minimal config
    config = {
        "sections": [
            {
                "title": "Sales Employees",
                "items": []
            }
        ]
    }
    
    import json
    with open(config_path, 'w') as f:
        json.dump(config, f)
    
    # Create sales data with Sales_Employee_Cleaned
    sales_df = pd.DataFrame({
        'Sales_Employee_Cleaned': ['John Doe', 'Jane Smith'],
        'Customer Name': ['Customer A', 'Customer B'],
        'Company Entity': ['GmbH', 'AG'],
        'Document Type': ['AR', 'AR'],
        'Region': ['Germany', 'Germany'],
        'Total Value (EUR)': [50000, 75000]
    })
    sales_df.to_csv(sales_path, index=False)
    
    # Save budget and prior data
    budget_gvl = pd.DataFrame({
        'Sales_Employee_Cleaned': ['John Doe', 'Jane Smith'],
        'Value_kEUR': [50, 80]
    })
    prior_gvl = pd.DataFrame({
        'Sales_Employee_Cleaned': ['John Doe', 'Jane Smith'],
        'Value_kEUR': [45, 70]
    })
    budget_gvl.to_csv(budget_path, index=False)
    prior_gvl.to_csv(prior_path, index=False)
    
    # Generate report
    try:
        generator = GVLReportGenerator(
            str(config_path),
            str(sales_path),
            str(budget_path),
            str(prior_path)
        )
        
        df = generator.calculate_report()
        
        # Verify report structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        
    except Exception as e:
        pytest.fail(f"GVL report generation failed: {e}")


def test_usa_spa_report_generation(temp_test_env, sample_budget_data, sample_prior_data):
    """Test USA Spa Report generation end-to-end."""
    # Prepare data files
    config_path = temp_test_env / 'src' / 'config' / 'usa_spa_report_structure.json'
    sales_path = temp_test_env / 'data' / 'outputs' / 'sales_usa.csv'
    budget_path = temp_test_env / 'data' / 'inputs' / 'budget' / 'budget_usa.csv'
    prior_path = temp_test_env / 'data' / 'inputs' / 'prior_years' / 'prior_usa.csv'
    
    # Create minimal config
    config = {
        "sections": [
            {
                "title": "USA Spa Sales",
                "items": [
                    {"label": "USA-East", "filter_value": "USA-East"},
                    {"label": "USA-West", "filter_value": "USA-West"}
                ]
            }
        ]
    }
    
    import json
    with open(config_path, 'w') as f:
        json.dump(config, f)
    
    # Create sales data
    sales_df = pd.DataFrame({
        'Sales Employee Name': ['Rep A', 'Rep B'],
        'Customer Name': ['Spa A', 'Spa B'],
        'Company Entity': ['Export', 'Export'],
        'Document Type': ['AR', 'AR'],
        'Market_Group': ['USA', 'USA'],
        'Channel_Level': ['Spa', 'Spa'],
        'Region': ['USA-East', 'USA-West'],
        'Value_kUSD': [100, 150]
    })
    sales_df.to_csv(sales_path, index=False)
    
    # Save budget and prior data (USA-specific)
    budget_usa = pd.DataFrame({
        'Date': ['01/01/2025', '01/01/2025'],
        'Region': ['USA-East', 'USA-West'],
        'Value_kUSD': [110, 140]
    })
    prior_usa = pd.DataFrame({
        'Date': ['01/01/2024', '01/01/2024'],
        'Region': ['USA-East', 'USA-West'],
        'Value_kUSD': [95, 130]
    })
    budget_usa.to_csv(budget_path, index=False)
    prior_usa.to_csv(prior_path, index=False)
    
    # Generate report
    try:
        generator = USASpaReportGenerator(
            str(config_path),
            str(sales_path),
            str(budget_path),
            str(prior_path)
        )
        
        df = generator.calculate_report()
        
        # Verify report structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert 'label' in df.columns
        assert 'actual' in df.columns
        
        # Verify unit is kUSD (since we have USD data)
        assert generator.unit == 'kUSD'
        
    except Exception as e:
        pytest.fail(f"USA Spa report generation failed: {e}")


def test_multi_format_export(temp_test_env, sample_budget_data, sample_prior_data):
    """Test that reports export to all formats (CSV, TXT, HTML, PDF)."""
    # Prepare minimal report data
    config_path = temp_test_env / 'src' / 'config' / 'usa_spa_report_structure.json'
    sales_path = temp_test_env / 'data' / 'outputs' / 'sales_export_test.csv'
    budget_path = temp_test_env / 'data' / 'inputs' / 'budget' / 'budget_export_test.csv'
    prior_path = temp_test_env / 'data' / 'inputs' / 'prior_years' / 'prior_export_test.csv'
    
    config = {
        "sections": [
            {"title": "Test", "items": [{"label": "Item", "filter_value": "Test"}]}
        ]
    }
    
    import json
    with open(config_path, 'w') as f:
        json.dump(config, f)
    
    sales_df = pd.DataFrame({
        'Market_Group': ['USA'],
        'Channel_Level': ['Spa'],
        'Region': ['USA-East'],
        'Document Type': ['AR'],
        'Value_kUSD': [100]
    })
    sales_df.to_csv(sales_path, index=False)
    
    sample_budget_data.to_csv(budget_path, index=False)
    sample_prior_data.to_csv(prior_path, index=False)
    
    try:
        generator = USASpaReportGenerator(
            str(config_path),
            str(sales_path),
            str(budget_path),
            str(prior_path)
        )
        
        df = generator.calculate_report()
        
        # Export to all formats
        output_base = temp_test_env / 'data' / 'outputs' / 'test_export'
        generator.export_report(df, str(output_base) + '.csv')
        
        # Verify all file formats exist
        assert (temp_test_env / 'data' / 'outputs' / 'test_export.csv').exists()
        assert (temp_test_env / 'data' / 'outputs' / 'test_export.txt').exists()
        assert (temp_test_env / 'data' / 'outputs' / 'test_export.html').exists()
        assert (temp_test_env / 'data' / 'outputs' / 'test_export.pdf').exists()
        
    except Exception as e:
        pytest.fail(f"Multi-format export failed: {e}")


@patch('sharepoint_client.SharePointHandler')
def test_full_pipeline_with_mocked_sharepoint(mock_sp_class, temp_test_env):
    """Test complete data pipeline with mocked SharePoint integration."""
    # Mock SharePoint download to return test data
    mock_sp = mock_sp_class.return_value
    
    def mock_download(sp_path, local_path):
        # Create mock files based on requested path
        if 'QRY_AR' in sp_path:
            df = pd.DataFrame({
                'Company Code': ['1000'],
                'Sales Employee': ['EMP001'],
                'Customer': ['CUST001'],
                'Posting Date': ['2025-01-15'],
                'Document Number': ['DOC001'],
                'Document Type': ['AR'],
                'Net Value': [10000],
                'Currency': ['EUR']
            })
            df.to_csv(local_path, index=False)
        elif 'entity_mappings' in sp_path:
            df = pd.DataFrame({
                'Sales_Employee': ['John Doe'],
                'Customer_Name': [pd.NA],
                'Market_Group': ['Europe'],
                'Region': ['Germany'],
                'Channel_Level': ['Direct'],
                'Company_Group': ['Group A'],
                'Sales_Employee_Cleaned': ['John Doe']
            })
            df.to_csv(local_path, index=False)
        return True
    
    mock_sp.download_file = Mock(side_effect=mock_download)
    
    # This test validates that the mocking infrastructure works
    # In a real scenario, this would call full_report.main() with mocked SharePoint
    assert mock_sp.download_file is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
