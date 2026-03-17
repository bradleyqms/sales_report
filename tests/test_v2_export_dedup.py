import importlib.util
from pathlib import Path
import sys

import pandas as pd

SCRIPT_PATH = Path(__file__).parent.parent / "src" / "v2_export_service.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
spec = importlib.util.spec_from_file_location("v2_export_service", SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

V2ExportService = module.V2ExportService


class DummyGenerator:
    def __init__(self):
        self.calls = 0

    def export_report(self, df, csv_path):
        self.calls += 1


def test_export_once_deduplicates_by_absolute_path(tmp_path):
    service = V2ExportService()
    generator = DummyGenerator()
    df = pd.DataFrame({"label": ["x"], "sales": [1.0]})

    output = tmp_path / "combined.csv"

    first = service.export_once(generator, df, output)
    second = service.export_once(generator, df, output)

    assert first is True
    assert second is False
    assert generator.calls == 1
