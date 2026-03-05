"""
One-time script to seed EntityMappings table from existing entity_mappings.csv file.
Run this after creating the database schema with Alembic.

Usage:
    python src/seed_mappings.py
"""

import os
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database import SessionLocal
from src.models import EntityMapping

# Load environment variables
load_dotenv()


def seed_entity_mappings():
    """
    Read entity_mappings.csv and bulk insert into EntityMappings table.
    """
    # Locate the CSV file
    csv_path = project_root / "data" / "inputs" / "mappings" / "entity_mappings.csv"
    
    if not csv_path.exists():
        print(f"❌ Error: entity_mappings.csv not found at {csv_path}")
        sys.exit(1)
    
    print(f"📂 Reading mappings from: {csv_path}")
    
    # Read CSV
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ Loaded {len(df)} rows from CSV")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        sys.exit(1)
    
    # Validate expected columns
    expected_cols = [
        'Entity', 'Market_Group', 'Region', 'Channel_Level', 
        'Company_Group', 'Sales_Employee', 'Customer_Code', 
        'Customer_Name', 'Sales_Employee_Cleaned'
    ]
    
    missing_cols = set(expected_cols) - set(df.columns)
    if missing_cols:
        missing_list = ", ".join(sorted(missing_cols))
        print(f"❌ Error: CSV is missing required columns: {missing_list}")
        sys.exit(1)
    print(f"✅ CSV columns validated")
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Check if table already has data
        existing_count = db.query(EntityMapping).count()
        if existing_count > 0:
            response = input(f"⚠️  Database already has {existing_count} mappings. Overwrite? (yes/no): ")
            if response.lower() != 'yes':
                print("❌ Aborted. No changes made.")
                return
            
            # Delete existing mappings
            print(f"🗑️  Deleting {existing_count} existing mappings...")
            db.query(EntityMapping).delete()
            db.commit()
        
        # Prepare mapping objects
        mappings = []
        for idx, row in df.iterrows():
            mapping = EntityMapping(
                entity=str(row['Entity']) if pd.notna(row['Entity']) else 'Descomed',
                market_group=str(row['Market_Group']) if pd.notna(row['Market_Group']) else '',
                region=str(row['Region']) if pd.notna(row['Region']) else '',
                sub_region=str(row.get('Sub Region', '')) if pd.notna(row.get('Sub Region')) else None,
                channel_level=str(row['Channel_Level']) if pd.notna(row['Channel_Level']) else '',
                company_group=str(row['Company_Group']) if pd.notna(row['Company_Group']) else None,
                sales_employee=str(row['Sales_Employee']) if pd.notna(row['Sales_Employee']) else None,
                customer_code=int(row['Customer_Code']) if pd.notna(row['Customer_Code']) else None,
                customer_name=str(row['Customer_Name']) if pd.notna(row['Customer_Name']) else None,
                sales_employee_cleaned=str(row['Sales_Employee_Cleaned']) if pd.notna(row['Sales_Employee_Cleaned']) else None,
                is_active=True,
                created_by='migration'
            )
            mappings.append(mapping)
        
        # Bulk insert
        print(f"💾 Inserting {len(mappings)} mappings into database...")
        db.bulk_save_objects(mappings)
        db.commit()
        
        print(f"✅ Successfully seeded {len(mappings)} entity mappings!")
        
        # Verify
        final_count = db.query(EntityMapping).count()
        print(f"✅ Verification: Database now has {final_count} total mappings")
        
        # Show sample
        sample = db.query(EntityMapping).limit(3).all()
        print("\n📋 Sample mappings:")
        for m in sample:
            print(f"  - ID {m.id}: {m.customer_name} (code: {m.customer_code}) -> {m.market_group}/{m.region}")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error during seeding: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    print("=" * 70)
    print("  Entity Mappings Seeding Script")
    print("=" * 70)
    print()
    
    seed_entity_mappings()
    
    print()
    print("=" * 70)
    print("  ✨ Seeding complete!")
    print("=" * 70)
