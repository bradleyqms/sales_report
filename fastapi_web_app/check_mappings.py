"""Check if a specific mapping exists and is active"""
import sys
from pathlib import Path
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(parent_dir / "src"))

from sqlalchemy.orm import sessionmaker
from src.models import EntityMapping
from src.database import engine

Session = sessionmaker(bind=engine)
session = Session()

try:
    # Check Liberty mappings
    mappings = session.query(EntityMapping).filter(
        EntityMapping.customer_name.ilike('%liberty%')
    ).all()
    
    print(f"\n📊 Found {len(mappings)} Liberty-related mappings:\n")
    for m in mappings:
        print(f"ID: {m.id}")
        print(f"  Customer: {m.customer_name} ({m.customer_code})")
        print(f"  Active: {m.is_active}")
        print(f"  Region: {m.region}")
        print(f"  Updated: {m.updated_at}")
        print("-" * 60)
    
except Exception as e:
    print(f"Error: {e}")
finally:
    session.close()
