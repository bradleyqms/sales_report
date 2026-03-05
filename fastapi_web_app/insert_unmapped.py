"""Manually insert the CSV unmapped entities into the database"""
import sys
from pathlib import Path
from datetime import datetime

parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(parent_dir / "src"))

from sqlalchemy.orm import sessionmaker
from src.models import UnmappedLog
from src.database import engine

Session = sessionmaker(bind=engine)
session = Session()

try:
    # Check if these entities already exist
    existing_liberty = session.query(UnmappedLog).filter(
        UnmappedLog.customer_code == '51157'
    ).first()
    
    existing_marcus = session.query(UnmappedLog).filter(
        UnmappedLog.customer_code == '25909'
    ).first()
    
    print(f"Liberty Professional (51157): {'EXISTS' if existing_liberty else 'NOT FOUND'}")
    print(f"Marcus Howard (25909): {'EXISTS' if existing_marcus else 'NOT FOUND'}")
    
    # Generate run_timestamp
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if not existing_liberty:
        print("\nCreating Liberty Professional...")
        liberty = UnmappedLog(
            entity_type='customer',
            customer_code='51157',
            entity_name='Liberty Professional',
            count=1,
            total_ar_value_keur=0.0,
            status='pending',
            sap_extract_files='EOM_February_2026.xlsx:UK',
            run_timestamp=run_timestamp,
            first_seen=datetime.now(),
            last_seen=datetime.now()
        )
        session.add(liberty)
    
    if not existing_marcus:
        print("Creating Marcus Howard...")
        marcus = UnmappedLog(
            entity_type='customer',
            customer_code='25909',
            entity_name='Marcus Howard',
            count=1,
            total_ar_value_keur=0.0,
            status='pending',
            sap_extract_files='EOM_February_2026.xlsx:Inc',
            run_timestamp=run_timestamp,
            first_seen=datetime.now(),
            last_seen=datetime.now()
        )
        session.add(marcus)
    
    session.commit()
    print("\n✅ Unmapped entities created successfully!")
    
    # Verify they're in the database
    total = session.query(UnmappedLog).filter_by(status='pending').count()
    print(f"\nTotal pending unmapped entities: {total}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    session.rollback()
finally:
    session.close()
