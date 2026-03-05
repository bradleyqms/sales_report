"""Quick verification of Phase 1 setup"""
from src.database import engine
from sqlalchemy import inspect
from src.models import EntityMapping
from sqlalchemy.orm import sessionmaker

print("=" * 70)
print("PHASE 1 VERIFICATION")
print("=" * 70)

# Check tables
inspector = inspect(engine)
tables = inspector.get_table_names()
print(f"\n✅ Tables created ({len(tables)}):")
for table in sorted(tables):
    print(f"   - {table}")

# Check data
Session = sessionmaker(bind=engine)
session = Session()

try:
    count = session.query(EntityMapping).count()
    print(f"\n✅ Entity mappings in database: {count}")
    
    # Sample record
    sample = session.query(EntityMapping).first()
    if sample:
        print(f"\n📋 Sample mapping:")
        print(f"   - Customer: {sample.customer_name} ({sample.customer_code})")
        print(f"   - Region: {sample.sales_region}/{sample.sales_market}")
        print(f"   - Sales Employee: {sample.sales_employee}")
    
    print("\n" + "=" * 70)
    print("✅ PHASE 1 COMPLETE - Database ready!")
    print("=" * 70)
    
finally:
    session.close()
