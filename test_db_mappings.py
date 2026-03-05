"""
Test script to verify database mapping loading works correctly.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

print("=" * 70)
print("Testing Database Mapping Load")
print("=" * 70)
print()

try:
    from src.qry_data_mapping import load_mappings_from_db
    
    print("Step 1: Loading mappings from database...")
    print("-" * 70)
    
    df = load_mappings_from_db()
    
    print(f"✅ Successfully loaded {len(df)} mappings")
    print()
    print("Columns:", list(df.columns))
    print()
    print("Sample mappings (first 5):")
    print(df.head())
    print()
    print("Data types:")
    print(df.dtypes)
    print()
    
    # Verify required columns exist
    required_cols = ['Sales_Employee', 'Customer_Name', 'Market_Group', 'Region', 'Channel_Level']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"❌ Missing required columns: {missing}")
        sys.exit(1)
    else:
        print(f"✅ All required columns present")
    
    # Check for null values in key columns
    print()
    print("Checking for data quality issues...")
    for col in ['Customer_Name', 'Market_Group', 'Region']:
        null_count = df[col].isna().sum()
        if null_count > 0:
            print(f"⚠️  {col}: {null_count} null values")
        else:
            print(f"✅ {col}: No null values")
    
    print()
    print("=" * 70)
    print("✅ Database mapping load test PASSED")
    print("=" * 70)
    
except Exception as e:
    print(f"❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
