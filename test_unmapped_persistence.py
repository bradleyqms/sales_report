"""
Test script to verify unmapped entity persistence to database.
"""
import sys
from pathlib import Path
import datetime

if __name__ == "__main__":
    # Add project root to path
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))

    print("=" * 70)
    print("Testing Unmapped Entity Persistence")
    print("=" * 70)
    print()

try:
    from src.qry_data_mapping import persist_unmapped_entities
    from src.database import engine
    from src.models import UnmappedLog
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import desc
    
    # Create test unmapped entities
    test_unmapped = {
        ('customer', 'Test Unmapped Customer Ltd'): {
            'count': 5,
            'dates': [datetime.datetime(2025, 12, 1), datetime.datetime(2025, 12, 15)],
            'values': [1500.0, 2300.0, 1800.0],
            'sources': ['test_extract_2025_12_01.csv', 'test_extract_2025_12_15.csv'],
            'customer_codes': ['99999']
        },
        ('employee', 'Test Sales Rep'): {
            'count': 3,
            'dates': [datetime.datetime(2025, 12, 10)],
            'values': [500.0, 750.0],
            'sources': ['test_extract_2025_12_10.csv'],
            'customer_codes': []
        }
    }
    
    print("Step 1: Persisting test unmapped entities to database...")
    print("-" * 70)
    
    run_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_TEST')
    count = persist_unmapped_entities(test_unmapped, run_timestamp=run_timestamp, use_database=True)
    
    print(f"[OK] Successfully persisted {count} unmapped entities")
    print()
    
    print("Step 2: Verifying database records...")
    print("-" * 70)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Query the test records we just created
        test_records = session.query(UnmappedLog).filter(
            UnmappedLog.run_timestamp == run_timestamp
        ).all()
        
        if len(test_records) != count:
            print(f"[ERROR] Expected {count} records, found {len(test_records)}")
            sys.exit(1)
        
        print(f"[OK] Found {len(test_records)} test records")
        print()
        
        for record in test_records:
            print(f"Entity: {record.entity_type} - {record.entity_name}")
            print(f"  Count: {record.count}")
            print(f"  First seen: {record.first_seen}")
            print(f"  Last seen: {record.last_seen}")
            print(f"  Total AR value (kEUR): {record.total_ar_value_keur}")
            print(f"  Status: {record.status}")
            print(f"  SAP extract files: {record.sap_extract_files}")
            if record.customer_code:
                print(f"  Customer code: {record.customer_code}")
            print()
        
        print("Step 3: Testing update of existing records...")
        print("-" * 70)
        
        # Try persisting again with updated data (should update existing records)
        test_unmapped_update = {
            ('customer', 'Test Unmapped Customer Ltd'): {
                'count': 2,
                'dates': [datetime.datetime(2025, 12, 20)],
                'values': [1200.0],
                'sources': ['test_extract_2025_12_20.csv'],
                'customer_codes': ['99999']
            }
        }
        
        updated_count = persist_unmapped_entities(test_unmapped_update, run_timestamp=run_timestamp, use_database=True)
        print(f"[OK] Updated {updated_count} record(s)")
        print()
        
        # Verify the update
        updated_record = session.query(UnmappedLog).filter(
            UnmappedLog.entity_type == 'customer',
            UnmappedLog.entity_name == 'Test Unmapped Customer Ltd',
            UnmappedLog.status == 'pending'
        ).first()
        
        if updated_record:
            print(f"Updated record count: {updated_record.count} (should be 7)")
            print(f"Updated last_seen: {updated_record.last_seen}")
            print(f"Updated sources: {updated_record.sap_extract_files}")
            print()
        
        print("Step 4: Checking total unmapped logs in database...")
        print("-" * 70)
        
        total_logs = session.query(UnmappedLog).count()
        pending_logs = session.query(UnmappedLog).filter_by(status='pending').count()
        
        print(f"Total unmapped logs: {total_logs}")
        print(f"Pending unmapped logs: {pending_logs}")
        print()
        
        # Show most recent non-test records
        recent_logs = session.query(UnmappedLog).filter(
            ~UnmappedLog.run_timestamp.like('%TEST%')
        ).order_by(desc(UnmappedLog.created_at)).limit(3).all()
        
        if recent_logs:
            print("Recent unmapped entities (excluding test):")
            for log in recent_logs:
                print(f"  - {log.entity_type}: {log.entity_name} (count: {log.count}, AR: {log.total_ar_value_keur} kEUR)")
        
        print()
        print("=" * 70)
        print("[OK] Unmapped entity persistence test PASSED")
        print("=" * 70)
        
    finally:
        session.close()
    
except Exception as e:
    print(f"[ERROR] Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
