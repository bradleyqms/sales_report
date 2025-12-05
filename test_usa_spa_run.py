import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.usa_spa_report import USASpaReportGenerator
import json

project_root = Path(__file__).parent
mapped_path = project_root / 'data' / 'outputs' / 'qry_unified_mapped_2025.csv'
budget_path = project_root / 'data' / 'inputs' / 'budget' / 'budget_2025_processed.csv'
prior_path = project_root / 'data' / 'inputs' / 'prior_years' / 'prior_sales_2024_usa.csv'
config_path = project_root / 'src' / 'config' / 'usa_spa_report_structure.json'

print('Using files:')
print(' mapped:', mapped_path.exists(), mapped_path)
print(' budget:', budget_path.exists(), budget_path)
print(' prior:', prior_path.exists(), prior_path)
print(' config:', config_path.exists(), config_path)

try:
    gen = USASpaReportGenerator(str(config_path), str(mapped_path), str(budget_path), str(prior_path))
    print('Generator unit:', getattr(gen, 'unit', 'UNSET'))
    df = gen.calculate_report()
    print('Report rows (top 10):')
    print(df.head(10).to_string(index=False))
except Exception as e:
    import traceback
    print('ERROR', e)
    traceback.print_exc()
