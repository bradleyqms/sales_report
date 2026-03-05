"""Quick test to check unmapped entities count"""
import sys
from pathlib import Path
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(parent_dir / "src"))

from sqlalchemy.orm import sessionmaker
from src.models import UnmappedLog
from src.database import engine

Session = sessionmaker(bind=engine)
session = Session()

try:
    count = session.query(UnmappedLog).count()
    print(f"Total: {count}")
    
    liberty = session.query(UnmappedLog).filter(UnmappedLog.customer_name.ilike('%Liberty%')).first()
    marcus = session.query(UnmappedLog).filter(UnmappedLog.customer_name.ilike('%Marcus%')).first()
    
    print(f"Liberty: {'Found' if liberty else 'Not found'}")
    print(f"Marcus: {'Found' if marcus else 'Not found'}")
finally:
    session.close()
