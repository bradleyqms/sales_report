import datetime
import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest

SCRIPT_PATH = Path(__file__).parent.parent / "src" / "v2_validation.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))
spec = importlib.util.spec_from_file_location("v2_validation", SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

ValidationError = module.ValidationError
validate_period_completeness = module.validate_period_completeness


def test_validate_period_completeness_eom_passes_with_month_end_date():
    df = pd.DataFrame({"Extract_Date": ["2026-02-01", "2026-02-28"]})
    warning = validate_period_completeness(
        df,
        report_type="EOM",
        report_date=datetime.datetime(2026, 2, 28),
        strict=True,
    )
    assert warning is None


def test_validate_period_completeness_eom_business_day_passes_with_last_weekday():
    # Feb 2026 ends on Saturday, so business-day policy expects Feb 27.
    df = pd.DataFrame({"Extract_Date": ["2026-02-01", "2026-02-27"]})
    warning = validate_period_completeness(
        df,
        report_type="EOM",
        report_date=datetime.datetime(2026, 2, 28),
        strict=True,
        eom_policy="business-day",
    )
    assert warning is None


def test_validate_period_completeness_eom_raises_when_not_complete():
    df = pd.DataFrame({"Extract_Date": ["2026-02-01", "2026-02-26"]})
    with pytest.raises(ValidationError):
        validate_period_completeness(
            df,
            report_type="EOM",
            report_date=datetime.datetime(2026, 2, 28),
            strict=True,
        )


def test_validate_period_completeness_eom_business_day_raises_when_mismatch():
    df = pd.DataFrame({"Extract_Date": ["2026-02-01", "2026-02-26"]})
    with pytest.raises(ValidationError):
        validate_period_completeness(
            df,
            report_type="EOM",
            report_date=datetime.datetime(2026, 2, 28),
            strict=True,
            eom_policy="business-day",
        )
