"""Test script to check unmapped entities in database"""
import sys
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(parent_dir / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import UnmappedLog
from src.database import engine

# Create session
Session = sessionmaker(bind=engine)
session = Session()

try:
    # Query all unmapped entities
    unmapped = session.query(UnmappedLog).all()
    
    print(f"\n📊 Total unmapped entities in database: {len(unmapped)}")
    print("="*80)
    
    if unmapped:
        print("\n🔍 First 10 unmapped entities:")
        for item in unmapped[:10]:
            print(f"  ID: {item.id}")
            print(f"  Customer Name: {item.customer_name}")
            print(f"  Customer Code: {item.customer_code}")
            print(f"  Sales Employee: {item.sales_employee}")
            print(f"  Count: {item.count}")
            print(f"  Status: {item.status}")
            print(f"  Total AR Value: {item.total_ar_value_keur}")
            print(f"  First Seen: {item.first_seen}")
            print(f"  Last Seen: {item.last_seen}")
            print("-"*80)
    
    # Check for the specific entities in the CSV
    print("\n🔎 Searching for specific entities from CSV...")
    
    liberty = session.query(UnmappedLog).filter(
        UnmappedLog.customer_name.ilike('%Liberty Professional%')
    ).first()
    
    marcus = session.query(UnmappedLog).filter(
        UnmappedLog.customer_name.ilike('%Marcus Howard%')
    ).first()
    
    print(f"  Liberty Professional: {'✅ FOUND' if liberty else '❌ NOT FOUND'}")
    print(f"  Marcus Howard: {'✅ FOUND' if marcus else '❌ NOT FOUND'}")
    
    if liberty:
        print(f"    Liberty - Customer Code: {liberty.customer_code}, Status: {liberty.status}")
    if marcus:
        print(f"    Marcus - Customer Code: {marcus.customer_code}, Status: {marcus.status}")
        
finally:
    session.close()
