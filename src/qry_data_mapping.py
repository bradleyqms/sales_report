import os
from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def apply_mappings(sales_df, mapping_df):
    """
    Applies entity mappings to the sales DataFrame.
    """
    # Validate mapping file has expected columns
    expected_cols = ['Sales_Employee', 'Customer_Name', 'Market_Group', 'Region', 'Channel_Level', 'Company_Group']
    missing_cols = [col for col in expected_cols if col not in mapping_df.columns]
    if missing_cols:
        logging.warning(f"Mapping file missing columns: {missing_cols}. Some mappings may fail.")
    
    # Clean mapping data
    mapping_df = mapping_df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

    # Apply mappings
    # No longer splitting into df_emp and df_cust; apply mappings to the entire df

    # 1. Employee Mapping (for GmbH/AG entities)
    if 'Sales_Employee' in mapping_df.columns:
        emp_cols = ['Sales_Employee', 'Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
        # Drop duplicates in mapping to avoid row explosion
        map_emp = mapping_df[emp_cols].dropna(subset=['Sales_Employee']).drop_duplicates(subset=['Sales_Employee'])
        
        # To apply only to GmbH/AG, set temp key
        sales_df['temp_employee'] = sales_df['Sales Employee Name']
        sales_df.loc[~sales_df['Company Entity'].isin(['GmbH', 'AG']), 'temp_employee'] = pd.NA
        sales_df = sales_df.merge(map_emp, left_on='temp_employee', right_on='Sales_Employee', how='left', suffixes=('', '_emp'))
        
        # Log unmapped employees
        unmapped_emp = sales_df[sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
        if not unmapped_emp.empty:
            logging.warning(f"Found {len(unmapped_emp)} unmapped employee records (GmbH/AG)")
        
        sales_df.drop('temp_employee', axis=1, inplace=True)

    # 2. Customer Mapping (for other entities)
    if 'Customer_Name' in mapping_df.columns:
        cust_cols = ['Customer_Name', 'Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
        # Drop duplicates in mapping
        map_cust = mapping_df[cust_cols].dropna(subset=['Customer_Name']).drop_duplicates(subset=['Customer_Name'])
        
        # Note: mapping file has 'Customer_Name', sales data has 'Customer Name'
        # To apply only to non-GmbH/AG, set temp key
        sales_df['temp_customer'] = sales_df['Customer Name']
        sales_df.loc[sales_df['Company Entity'].isin(['GmbH', 'AG']), 'temp_customer'] = pd.NA
        sales_df = sales_df.merge(map_cust, left_on='temp_customer', right_on='Customer_Name', how='left', suffixes=('', '_cust'))
        
        # Log unmapped customers
        unmapped_cust = sales_df[~sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
        if not unmapped_cust.empty:
            logging.warning(f"Found {len(unmapped_cust)} unmapped customer records (non-GmbH/AG)")
        
        sales_df.drop('temp_customer', axis=1, inplace=True)

    # Combine the mappings: for common columns, prefer emp if available, else cust
    common_cols = ['Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
    for col in common_cols:
        if col + '_cust' in sales_df.columns:
            sales_df[col] = sales_df[col].fillna(sales_df[col + '_cust'])
            sales_df.drop(col + '_cust', axis=1, inplace=True)

    # Sales_Employee_Cleaned is now from both emp and cust mappings

    # Drop the merge keys if added
    for col in ['Sales_Employee', 'Customer_Name']:
        if col in sales_df.columns:
            sales_df.drop(col, axis=1, inplace=True)

    # For Export entity, keep only AR rows (for QRY data, Document Type is 'AR', not 'AR Invoice')
    sales_df = sales_df[~((sales_df['Company Entity'] == 'Export') & (sales_df['Document Type'] != 'AR'))]
    
    # For rows with Region == 'Switzerland', keep only AG entity
    if 'Region' in sales_df.columns:
        sales_df = sales_df[~((sales_df['Region'] == 'Switzerland') & (sales_df['Company Entity'] != 'AG'))]

    # Filter out rows where Customer Name contains "Interco"
    sales_df = sales_df[~sales_df['Customer Name'].str.contains('Interco', case=False, na=False)]

    # Map Channel_Level 'eCommerce (excl. USA)' to 'eCommerce EU (incl. UK)'
    if 'Channel_Level' in sales_df.columns:
        sales_df['Channel_Level'] = sales_df['Channel_Level'].replace('eCommerce (excl. USA)', 'eCommerce EU (incl. UK)')

    # Also map in Sales_Employee_Cleaned and Region if present
    if 'Sales_Employee_Cleaned' in sales_df.columns:
        sales_df['Sales_Employee_Cleaned'] = sales_df['Sales_Employee_Cleaned'].replace('eCommerce (excl. USA)', 'eCommerce EU (incl. UK)')
    if 'Region' in sales_df.columns:
        sales_df['Region'] = sales_df['Region'].replace('eCommerce (excl. USA)', 'eCommerce EU (incl. UK)')
        
    return sales_df

if __name__ == "__main__":
    # Get the inputs folder path
    inputs_folder = Path(__file__).parent.parent / "data/inputs"
    outputs_folder = Path(__file__).parent.parent / "data/outputs"

    # Find the mapping file
    mapping_file = inputs_folder / "mappings/entity_mappings.csv"

    # Read the data
    sales_df = pd.read_csv(outputs_folder / "qry_unified_2025.csv")

    if mapping_file.suffix.lower() == '.xlsx':
        mapping_df = pd.read_excel(mapping_file)
    elif mapping_file.suffix.lower() == '.csv':
        mapping_df = pd.read_csv(mapping_file)
    else:
        print("Unsupported mapping file format")
        exit(1)
        
    mapped_df = apply_mappings(sales_df, mapping_df)

    # Save output
    output_path = outputs_folder / "qry_unified_mapped_2025.csv"
    mapped_df.to_csv(output_path, index=False)
    print(f"Mapped QRY data saved to {output_path}")
    print("Sample of mapped data:")
    print(mapped_df.head(10))