import os
from pathlib import Path
import pandas as pd
import logging
import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def apply_mappings(sales_df, mapping_df, output_dir=None):
    """
    Applies entity mappings to the sales DataFrame.
    
    Args:
        sales_df: DataFrame containing sales data
        mapping_df: DataFrame containing entity mappings
        output_dir: Optional path to output directory for unmapped entities CSV.
                   If None, defaults to ../data/outputs relative to this file.
    
    Returns:
        Mapped sales DataFrame
        
    Side Effects:
        Exports unmapped_entities_{timestamp}.csv to output_dir with:
        - entity_type: 'customer' or 'employee'
        - entity_name: Name of unmapped entity
        - count: Number of records for this entity
        - first_seen: Earliest date in data
        - last_seen: Latest date in data
    """
    # Initialize unmapped entity tracking
    unmapped_entities = defaultdict(lambda: {'count': 0, 'dates': []})
    
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
        
        # Track unmapped employees
        unmapped_emp = sales_df[sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
        if not unmapped_emp.empty:
            logging.warning(f"Found {len(unmapped_emp)} unmapped employee records (GmbH/AG)")
            for _, row in unmapped_emp.iterrows():
                emp_name = str(row.get('Sales Employee Name', 'Unknown')).strip()
                if emp_name and emp_name not in ['nan', 'None', '']:
                    key = ('employee', emp_name)
                    unmapped_entities[key]['count'] += 1
                    if 'Posting Date' in row and pd.notna(row['Posting Date']):
                        unmapped_entities[key]['dates'].append(row['Posting Date'])
        
        sales_df.drop('temp_employee', axis=1, inplace=True)

    # 2. Customer Mapping (for other entities)
    if 'Customer_Name' in mapping_df.columns and 'Customer Name' in sales_df.columns:
        cust_cols = ['Customer_Name', 'Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
        # Drop duplicates in mapping
        map_cust = mapping_df[cust_cols].dropna(subset=['Customer_Name']).drop_duplicates(subset=['Customer_Name'])
        
        # Note: mapping file has 'Customer_Name', sales data has 'Customer Name'
        # To apply only to non-GmbH/AG, set temp key
        sales_df['temp_customer'] = sales_df['Customer Name']
        sales_df.loc[sales_df['Company Entity'].isin(['GmbH', 'AG']), 'temp_customer'] = pd.NA
        sales_df = sales_df.merge(map_cust, left_on='temp_customer', right_on='Customer_Name', how='left', suffixes=('', '_cust'))
        
        # Attempt to resolve unmapped customers using Sales Employee exact matches
        # (accept only perfect/equivalent-to-1.0 matches)
        if 'Sales Employee Name' in sales_df.columns:
            # Build lookup maps from the mapping rows
            cust_lookup = {}
            emp_lookup = {}
            for _, r in map_cust.iterrows():
                cname = str(r.get('Customer_Name', '')).strip()
                if cname:
                    cust_lookup[cname] = r
            # If an employee mapping (map_emp) exists, use it to build emp_lookup
            if 'map_emp' in locals():
                for _, r2 in map_emp.iterrows():
                    se_val = r2.get('Sales_Employee')
                    if pd.notna(se_val):
                        emp_lookup[str(se_val).strip()] = r2

            # For rows still without Market_Group, try exact match against Sales Employee Name
            mask_unmapped = (~sales_df['Company Entity'].isin(['GmbH', 'AG'])) & (sales_df['Market_Group'].isna())
            for idx in sales_df[mask_unmapped].index:
                se_name = str(sales_df.at[idx, 'Sales Employee Name']).strip() if 'Sales Employee Name' in sales_df.columns else ''
                cust_key = str(sales_df.at[idx, 'temp_customer']).strip() if 'temp_customer' in sales_df.columns else ''
                row = None
                # Prefer exact customer-name lookup
                if cust_key and cust_key in cust_lookup:
                    row = cust_lookup[cust_key]
                # Next, try to match Sales Employee name against mapping rows (exact match only)
                elif se_name and se_name in emp_lookup:
                    row = emp_lookup[se_name]
                # Also allow customer name matching against employee-mapped rows
                elif cust_key and cust_key in emp_lookup:
                    row = emp_lookup[cust_key]

                if row is not None:
                    for col in ['Market_Group', 'Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']:
                        if col in row and pd.notna(row[col]):
                            sales_df.at[idx, col] = row[col]

        # Track unmapped customers after attempting Sales Employee matches
        unmapped_cust = sales_df[~sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
        if not unmapped_cust.empty:
            logging.warning(f"Found {len(unmapped_cust)} unmapped customer records (non-GmbH/AG)")
            for _, row in unmapped_cust.iterrows():
                cust_name = str(row.get('Customer Name', 'Unknown')).strip()
                if cust_name and cust_name not in ['nan', 'None', '']:
                    key = ('customer', cust_name)
                    unmapped_entities[key]['count'] += 1
                    if 'Posting Date' in row and pd.notna(row['Posting Date']):
                        unmapped_entities[key]['dates'].append(row['Posting Date'])

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
    if 'Company Entity' in sales_df.columns and 'Document Type' in sales_df.columns and len(sales_df) > 0:
        sales_df = sales_df[~((sales_df['Company Entity'] == 'Export') & (sales_df['Document Type'] != 'AR'))]
    
    # For rows with Region == 'Switzerland', keep only AG entity
    if 'Region' in sales_df.columns and 'Company Entity' in sales_df.columns and len(sales_df) > 0:
        sales_df = sales_df[~((sales_df['Region'] == 'Switzerland') & (sales_df['Company Entity'] != 'AG'))]

    # Filter out rows where Customer Name contains "Interco"
    if 'Customer Name' in sales_df.columns and len(sales_df) > 0:
        sales_df = sales_df[~sales_df['Customer Name'].str.contains('Interco', case=False, na=False)]

    # Map Channel_Level 'eCommerce (excl. USA)' to 'eCommerce EU (incl. UK)'
    if 'Channel_Level' in sales_df.columns:
        sales_df['Channel_Level'] = sales_df['Channel_Level'].replace('eCommerce (excl. USA)', 'eCommerce EU (incl. UK)')

    # Also map in Sales_Employee_Cleaned and Region if present
    if 'Sales_Employee_Cleaned' in sales_df.columns:
        sales_df['Sales_Employee_Cleaned'] = sales_df['Sales_Employee_Cleaned'].replace('eCommerce (excl. USA)', 'eCommerce EU (incl. UK)')
    if 'Region' in sales_df.columns:
        sales_df['Region'] = sales_df['Region'].replace('eCommerce (excl. USA)', 'eCommerce EU (incl. UK)')
    
    # Export unmapped entities to CSV
    if unmapped_entities:
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / "data" / "outputs"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Build unmapped entities DataFrame
        unmapped_records = []
        for (entity_type, entity_name), data in unmapped_entities.items():
            record = {
                'entity_type': entity_type,
                'entity_name': entity_name,
                'count': data['count']
            }
            
            # Calculate first_seen and last_seen from dates
            if data['dates']:
                try:
                    # Parse dates if they're strings
                    dates = []
                    for d in data['dates']:
                        if isinstance(d, str):
                            try:
                                dates.append(pd.to_datetime(d))
                            except:
                                pass
                        elif isinstance(d, (pd.Timestamp, datetime.datetime)):
                            dates.append(pd.Timestamp(d))
                    
                    if dates:
                        record['first_seen'] = min(dates).strftime('%Y-%m-%d')
                        record['last_seen'] = max(dates).strftime('%Y-%m-%d')
                    else:
                        record['first_seen'] = 'N/A'
                        record['last_seen'] = 'N/A'
                except Exception:
                    record['first_seen'] = 'N/A'
                    record['last_seen'] = 'N/A'
            else:
                record['first_seen'] = 'N/A'
                record['last_seen'] = 'N/A'
            
            unmapped_records.append(record)
        
        unmapped_df = pd.DataFrame(unmapped_records)
        unmapped_df = unmapped_df.sort_values(['entity_type', 'count'], ascending=[True, False])
        
        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        unmapped_path = output_dir / f"unmapped_entities_{timestamp}.csv"
        unmapped_df.to_csv(unmapped_path, index=False)
        
        logging.info(f"Exported {len(unmapped_records)} unmapped entities to {unmapped_path}")
        logging.info(f"Unmapped summary: {unmapped_df['entity_type'].value_counts().to_dict()}")
    else:
        logging.info("No unmapped entities found - all entities successfully mapped!")
        
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