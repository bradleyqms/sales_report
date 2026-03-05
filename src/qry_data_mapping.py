import os
from pathlib import Path
import pandas as pd
import numpy as np
import logging
import datetime
from collections import defaultdict
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_mappings_from_db():
    """
    Load entity mappings from Azure SQL Database.
    
    Returns:
        DataFrame with columns matching the CSV format:
        - Sales_Employee
        - Customer_Name
        - Market_Group
        - Region
        - Sub Region
        - Channel_Level
        - Company_Group
        - Sales_Employee_Cleaned
        
    Raises:
        Exception: If database connection fails or no mappings found
    """
    try:
        from .database import engine
        from .models import EntityMapping
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Query active mappings only
            mappings = session.query(EntityMapping).filter_by(is_active=True).all()
            
            if not mappings:
                raise ValueError("No active entity mappings found in database")
            
            # Convert to DataFrame matching CSV structure
            data = []
            for m in mappings:
                data.append({
                    'Sales_Employee': m.sales_employee,
                    'Customer_Name': m.customer_name,
                    'Customer_Code': m.customer_code,
                    'Market_Group': m.market_group,
                    'Region': m.region,
                    'Sub Region': m.sub_region,
                    'Channel_Level': m.channel_level,
                    'Company_Group': m.company_group,
                    'Sales_Employee_Cleaned': m.sales_employee_cleaned if m.sales_employee_cleaned else m.sales_employee
                })
            
            df = pd.DataFrame(data)
            logging.info(f"✅ Loaded {len(df)} entity mappings from database")
            return df
            
        finally:
            session.close()
            
    except ImportError:
        logging.error("❌ Database modules not available. Install requirements: pip install sqlalchemy pyodbc azure-identity")
        raise
    except Exception as e:
        logging.error(f"❌ Failed to load mappings from database: {e}")
        raise

def persist_unmapped_entities(unmapped_entities, run_timestamp=None, use_database=False):
    """
    Persist unmapped entity statistics to the database.
    
    Args:
        unmapped_entities: Dict of {(entity_type, entity_name): stats} from collect_unmapped_stats
        run_timestamp: Optional run identifier (defaults to current timestamp)
        use_database: If True, persist to database; if False, skip persistence
        
    Returns:
        int: Number of unmapped entities persisted
    """
    if not use_database:
        return 0
    
    if not unmapped_entities:
        logging.info("No unmapped entities to persist")
        return 0
    
    try:
        from .database import engine
        from .models import UnmappedLog
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy import and_
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        if run_timestamp is None:
            run_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
        persisted_count = 0
        updated_count = 0
        
        try:
            for (entity_type, entity_name), data in unmapped_entities.items():
                # Parse dates
                dates = []
                if data.get('dates'):
                    for d in data['dates']:
                        if isinstance(d, str):
                            try:
                                parsed = pd.to_datetime(d)
                                # Make timezone-aware if not already
                                if parsed.tzinfo is None:
                                    parsed = parsed.tz_localize('UTC')
                                dates.append(parsed)
                            except:
                                pass
                        elif isinstance(d, (pd.Timestamp, datetime.datetime)):
                            ts = pd.Timestamp(d)
                            # Make timezone-aware if not already
                            if ts.tzinfo is None:
                                ts = ts.tz_localize('UTC')
                            dates.append(ts)
                
                first_seen = min(dates) if dates else None
                last_seen = max(dates) if dates else None
                
                # Calculate total AR value
                total_ar_value = 0.0
                if data.get('values'):
                    try:
                        total_value = sum([float(v) for v in data['values'] if pd.notna(v)])
                        total_ar_value = round(total_value / 1000, 2) if total_value > 100 else round(total_value, 2)
                    except Exception:
                        total_ar_value = 0.0
                
                # Get unique source files
                sap_extract_files = None
                if data.get('sources'):
                    unique_sources = list(set([s for s in data['sources'] if s and s not in ['nan', 'None', '']]))
                    if unique_sources:
                        sap_extract_files = '; '.join(sorted(unique_sources))
                
                # Get customer code
                customer_code = None
                if data.get('customer_codes'):
                    unique_codes = list(set([c for c in data['customer_codes'] if c and c not in ['nan', 'None', '']]))
                    if unique_codes:
                        customer_code = '; '.join(sorted(unique_codes))
                
                # Check if this entity already exists in pending state
                existing = session.query(UnmappedLog).filter(
                    and_(
                        UnmappedLog.entity_type == entity_type,
                        UnmappedLog.entity_name == entity_name,
                        UnmappedLog.status == 'pending'
                    )
                ).first()
                
                if existing:
                    # Update existing record
                    existing.count += data['count']
                    
                    # Handle timezone-aware date comparisons
                    if first_seen:
                        if not existing.first_seen:
                            existing.first_seen = first_seen
                        else:
                            # Ensure both are timezone-aware for comparison
                            existing_first = existing.first_seen
                            if existing_first.tzinfo is None:
                                existing_first = existing_first.replace(tzinfo=datetime.timezone.utc)
                            if first_seen < existing_first:
                                existing.first_seen = first_seen
                    
                    if last_seen:
                        if not existing.last_seen:
                            existing.last_seen = last_seen
                        else:
                            # Ensure both are timezone-aware for comparison
                            existing_last = existing.last_seen
                            if existing_last.tzinfo is None:
                                existing_last = existing_last.replace(tzinfo=datetime.timezone.utc)
                            if last_seen > existing_last:
                                existing.last_seen = last_seen
                    
                    existing.total_ar_value_keur += total_ar_value
                    existing.run_timestamp = run_timestamp
                    if sap_extract_files:
                        # Merge source files
                        if existing.sap_extract_files:
                            combined = set(existing.sap_extract_files.split('; ')) | set(sap_extract_files.split('; '))
                            existing.sap_extract_files = '; '.join(sorted(combined))
                        else:
                            existing.sap_extract_files = sap_extract_files
                    updated_count += 1
                else:
                    # Create new record
                    unmapped_log = UnmappedLog(
                        entity_type=entity_type,
                        entity_name=entity_name,
                        customer_code=customer_code,
                        count=data['count'],
                        first_seen=first_seen,
                        last_seen=last_seen,
                        total_ar_value_keur=total_ar_value,
                        sap_extract_files=sap_extract_files,
                        status='pending',
                        run_timestamp=run_timestamp
                    )
                    session.add(unmapped_log)
                    persisted_count += 1
            
            session.commit()
            
            total = persisted_count + updated_count
            logging.info(f"✅ Persisted {total} unmapped entities to database ({persisted_count} new, {updated_count} updated)")
            return total
            
        except Exception as e:
            session.rollback()
            logging.error(f"❌ Failed to persist unmapped entities: {e}")
            raise
        finally:
            session.close()
            
    except ImportError:
        logging.warning("⚠️  Database modules not available - skipping unmapped entity persistence")
        return 0
    except Exception as e:
        logging.error(f"❌ Failed to persist unmapped entities: {e}")
        return 0

def collect_unmapped_stats(unmapped_df, entity_type, entity_col):
    """
    Vectorized collection of unmapped entity statistics.
    Returns a dict of {(entity_type, entity_name): stats} entries.
    """
    if unmapped_df.empty:
        return {}
    
    result = {}
    
    # Get entity names
    entity_names = unmapped_df[entity_col].fillna('Unknown').astype(str).str.strip()
    valid_mask = ~entity_names.isin(['nan', 'None', ''])
    
    if not valid_mask.any():
        return {}
    
    working_df = unmapped_df[valid_mask].copy()
    working_df['_entity_name'] = entity_names[valid_mask]
    
    # Get value column
    if 'Value_in_EUR_converted' in working_df.columns:
        val_col = 'Value_in_EUR_converted'
    elif 'Total Value (EUR)' in working_df.columns:
        val_col = 'Total Value (EUR)'
    else:
        val_col = None
    
    # Group by entity name and aggregate
    for entity_name, group in working_df.groupby('_entity_name'):
        key = (entity_type, entity_name)
        stats = {
            'count': len(group),
            'dates': [],
            'values': [],
            'sources': [],
            'customer_codes': []
        }
        
        # Collect values
        if val_col and val_col in group.columns:
            stats['values'] = group[val_col].dropna().tolist()
        
        # Collect source files
        if 'Source_File' in group.columns:
            stats['sources'] = group['Source_File'].dropna().tolist()
        
        # Collect customer codes (for customers only)
        if entity_type == 'customer' and 'Customer Code' in group.columns:
            stats['customer_codes'] = group['Customer Code'].dropna().astype(str).tolist()
        
        # Collect dates
        if 'Posting Date' in group.columns:
            stats['dates'] = group['Posting Date'].dropna().tolist()
        
        result[key] = stats
    
    return result

def apply_mappings(sales_df, mapping_df=None, output_dir=None, use_database=False, persist_to_db=False):
    """
    Applies entity mappings to the sales DataFrame.
    
    Args:
        sales_df: DataFrame containing sales data
        mapping_df: DataFrame containing entity mappings (optional if use_database=True)
        output_dir: Optional path to output directory for unmapped entities CSV.
                   If None, defaults to ../data/outputs relative to this file.
        use_database: If True, load mappings from database instead of using mapping_df
        persist_to_db: If True, persist unmapped entities to database (in addition to CSV export)
    
    Returns:
        Mapped sales DataFrame
        
    Side Effects:
        Exports unmapped_entities_{timestamp}.csv to output_dir with:
        - entity_type: 'customer' or 'employee'
        - entity_name: Name of unmapped entity
        - count: Number of records for this entity
        - first_seen: Earliest date in data
        - last_seen: Latest date in data
        
        If persist_to_db=True, also persists unmapped entities to UnmappedLog table
    """
    # Load mappings from database if requested
    if use_database:
        logging.info("📊 Loading entity mappings from database...")
        mapping_df = load_mappings_from_db()
    elif mapping_df is None:
        raise ValueError("Either mapping_df must be provided or use_database must be True")
    
    # Initialize unmapped entity tracking with expanded fields
    unmapped_entities = defaultdict(lambda: {'count': 0, 'dates': [], 'values': [], 'sources': [], 'customer_codes': [], 'rows': []})
    
    # Validate mapping file has expected columns
    expected_cols = ['Sales_Employee', 'Customer_Name', 'Market_Group', 'Region', 'Channel_Level', 'Company_Group']
    missing_cols = [col for col in expected_cols if col not in mapping_df.columns]
    if missing_cols:
        logging.warning(f"Mapping file missing columns: {missing_cols}. Some mappings may fail.")
    
    # Clean mapping data - vectorized string stripping
    for col in mapping_df.select_dtypes(include=['object']).columns:
        mapping_df[col] = mapping_df[col].str.strip()

    # Apply mappings
    # No longer splitting into df_emp and df_cust; apply mappings to the entire df

    # 1. Employee Mapping (for GmbH/AG entities)
    if 'Sales_Employee' in mapping_df.columns:
        emp_cols = ['Sales_Employee', 'Market_Group', 'Region', 'Sub Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
        # Drop duplicates in mapping to avoid row explosion
        map_emp = mapping_df[emp_cols].dropna(subset=['Sales_Employee']).drop_duplicates(subset=['Sales_Employee'])
        
        # To apply only to GmbH/AG, set temp key
        sales_df['temp_employee'] = sales_df['Sales Employee Name']
        sales_df.loc[~sales_df['Company Entity'].isin(['GmbH', 'AG']), 'temp_employee'] = pd.NA
        sales_df = sales_df.merge(map_emp, left_on='temp_employee', right_on='Sales_Employee', how='left', suffixes=('', '_emp'))
        
        # Track unmapped employees with AR values and source files (VECTORIZED)
        unmapped_emp = sales_df[sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
        if not unmapped_emp.empty:
            logging.warning(f"Found {len(unmapped_emp)} unmapped employee records (GmbH/AG)")
            emp_stats = collect_unmapped_stats(unmapped_emp, 'employee', 'Sales Employee Name')
            unmapped_entities.update(emp_stats)
        
        sales_df.drop('temp_employee', axis=1, inplace=True)

    # 2. Customer Mapping (for other entities)
    if 'Customer_Name' in mapping_df.columns and 'Customer Name' in sales_df.columns:
        cust_cols = ['Customer_Name', 'Market_Group', 'Region', 'Sub Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
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

        # Track unmapped customers after attempting Sales Employee matches (VECTORIZED)
        unmapped_cust = sales_df[~sales_df['Company Entity'].isin(['GmbH', 'AG']) & sales_df['Market_Group'].isna()]
        if not unmapped_cust.empty:
            logging.warning(f"Found {len(unmapped_cust)} unmapped customer records (non-GmbH/AG)")
            cust_stats = collect_unmapped_stats(unmapped_cust, 'customer', 'Customer Name')
            unmapped_entities.update(cust_stats)

        sales_df.drop('temp_customer', axis=1, inplace=True)

    # Combine the mappings: for common columns, prefer emp if available, else cust
    common_cols = ['Market_Group', 'Region', 'Sub Region', 'Channel_Level', 'Company_Group', 'Sales_Employee_Cleaned']
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
    
    # Persist unmapped entities to database (if enabled)
    if unmapped_entities and persist_to_db:
        run_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        persist_unmapped_entities(unmapped_entities, run_timestamp=run_timestamp, use_database=True)
    
    # Export unmapped entities to CSV
    if unmapped_entities:
        if output_dir is None:
            import os as _os
            _env = _os.environ.get('REPORT_OUTPUT_DIR')
            output_dir = Path(_env) if _env else Path(__file__).parent.parent / "data" / "outputs"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Build unmapped entities DataFrame with extended info
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
            
            # Calculate total AR value (kEUR)
            if data['values']:
                try:
                    total_value = sum([float(v) for v in data['values'] if pd.notna(v)])
                    record['total_ar_value_keur'] = round(total_value / 1000, 2) if total_value > 100 else round(total_value, 2)
                except Exception:
                    record['total_ar_value_keur'] = 'N/A'
            else:
                record['total_ar_value_keur'] = 0
            
            # List unique source files
            if data['sources']:
                unique_sources = list(set([s for s in data['sources'] if s and s not in ['nan', 'None', '']]))
                record['sap_extract_files'] = '; '.join(sorted(unique_sources)) if unique_sources else 'Unknown'
            else:
                record['sap_extract_files'] = 'Unknown'
            
            # Customer code - only for customers, empty for employees
            if data['customer_codes']:
                unique_codes = list(set([c for c in data['customer_codes'] if c and c not in ['nan', 'None', '']]))
                record['customer_code'] = '; '.join(sorted(unique_codes)) if unique_codes else ''
            else:
                record['customer_code'] = ''
            
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

    # Read the sales data
    sales_df = pd.read_csv(outputs_folder / "qry_unified_2025.csv")

    # Try to use database first, fall back to CSV if database unavailable
    try:
        logging.info("Attempting to load mappings from database...")
        mapped_df = apply_mappings(sales_df, use_database=True, persist_to_db=True, output_dir=outputs_folder)
        logging.info("✅ Successfully used database for entity mappings and unmapped entity tracking")
    except Exception as e:
        logging.warning(f"⚠️  Database unavailable, falling back to CSV: {e}")
        
        # Fall back to CSV file
        mapping_file = inputs_folder / "mappings/entity_mappings.csv"
        
        if not mapping_file.exists():
            logging.error(f"❌ Mapping file not found: {mapping_file}")
            exit(1)
        
        if mapping_file.suffix.lower() == '.xlsx':
            mapping_df = pd.read_excel(mapping_file)
        elif mapping_file.suffix.lower() == '.csv':
            mapping_df = pd.read_csv(mapping_file)
        else:
            print("Unsupported mapping file format")
            exit(1)
        
        mapped_df = apply_mappings(sales_df, mapping_df, output_dir=outputs_folder)
        logging.info("✅ Used CSV fallback for entity mappings")

    # Save output
    output_path = outputs_folder / "qry_unified_mapped_2025.csv"
    mapped_df.to_csv(output_path, index=False)
    print(f"Mapped QRY data saved to {output_path}")
    print("Sample of mapped data:")
    print(mapped_df.head(10))