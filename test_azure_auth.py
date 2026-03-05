"""
Quick test script to verify Azure AD authentication setup.
Run this after granting database permissions to your Azure AD account.

Usage:
    python test_azure_auth.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

print("=" * 70)
print("  Azure AD Authentication Test")
print("=" * 70)
print()

print("Step 1: Testing Azure AD token acquisition...")
print("-" * 70)

try:
    from src.database import get_azure_sql_token
    token = get_azure_sql_token()
    print("✅ Successfully acquired Azure AD token!")
    print(f"   Token length: {len(token)} bytes")
except Exception as e:
    print(f"❌ Failed to acquire token: {e}")
    print()
    print("💡 Make sure you've run: az login")
    sys.exit(1)

print()
print("Step 2: Testing database connection...")
print("-" * 70)

try:
    from src.database import test_connection
    result = test_connection()
    if result:
        print("✅ Database connection successful!")
    else:
        print("❌ Database connection failed (see errors above)")
        sys.exit(1)
except Exception as e:
    print(f"❌ Connection test failed: {e}")
    print()
    print("💡 Common issues:")
    print("   1. Run SQL commands to grant database permissions to your Azure AD account")
    print("   2. Check your IP is whitelisted in Azure SQL firewall")
    print("   3. Verify ODBC Driver 18 is installed")
    sys.exit(1)

print()
print("Step 3: Verifying database models...")
print("-" * 70)

try:
    from src.database import engine
    from sqlalchemy import inspect
    
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    if tables:
        print(f"✅ Found {len(tables)} existing tables:")
        for table in tables:
            print(f"   - {table}")
    else:
        print("⚠️  No tables found yet (expected before running migrations)")
except Exception as e:
    print(f"⚠️  Could not inspect database: {e}")

print()
print("=" * 70)
print("  ✨ Azure AD Authentication Setup Complete!")
print("=" * 70)
print()
print("Next steps:")
print("  1. Run: python -m alembic revision --autogenerate -m 'initial_schema'")
print("  2. Run: python -m alembic upgrade head")
print("  3. Run: python src/seed_mappings.py")
print()
